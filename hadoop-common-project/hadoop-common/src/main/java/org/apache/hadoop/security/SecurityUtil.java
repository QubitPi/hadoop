/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_DNS_INTERFACE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_DNS_NAMESERVER_KEY;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.naming.ConfigurationException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.DomainNameResolver;
import org.apache.hadoop.net.DomainNameResolverFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.Name;
import org.xbill.DNS.ResolverConfig;


import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;

/**
 * Security Utils.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class SecurityUtil {
  public static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);
  public static final String HOSTNAME_PATTERN = "_HOST";
  public static final String FAILED_TO_GET_UGI_MSG_HEADER = 
      "Failed to obtain user group information:";

  private SecurityUtil() {
  }

  // controls whether buildTokenService will use an ip or host/ip as given
  // by the user
  @VisibleForTesting
  static boolean useIpForTokenService;
  @VisibleForTesting
  static HostResolver hostResolver;

  private static DomainNameResolver domainNameResolver;

  private static boolean logSlowLookups;
  private static int slowLookupThresholdMs;
  private static long cachingInterval = 0;

  static {
    setConfigurationInternal(new Configuration());
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void setConfiguration(Configuration conf) {
    LOG.info("Updating Configuration");
    setConfigurationInternal(conf);
  }

  private static void setConfigurationInternal(Configuration conf) {
    boolean useIp = conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP,
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT);
    cachingInterval = conf.getTimeDuration(
        CommonConfigurationKeys.HADOOP_SECURITY_HOSTNAME_CACHE_EXPIRE_INTERVAL_SECONDS,
        CommonConfigurationKeys.HADOOP_SECURITY_HOSTNAME_CACHE_EXPIRE_INTERVAL_SECONDS_DEFAULT,
        TimeUnit.SECONDS);
    setTokenServiceUseIp(useIp);

    logSlowLookups = conf.getBoolean(
        CommonConfigurationKeys
            .HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_ENABLED_KEY,
        CommonConfigurationKeys
            .HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_ENABLED_DEFAULT);

    slowLookupThresholdMs = conf.getInt(
        CommonConfigurationKeys
            .HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
        CommonConfigurationKeys
            .HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

    domainNameResolver = DomainNameResolverFactory.newInstance(conf,
        CommonConfigurationKeys.HADOOP_SECURITY_RESOLVER_IMPL);
  }

  /**
   * For use only by tests and initialization.
   *
   * @param flag flag.
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  public static void setTokenServiceUseIp(boolean flag) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting "
          + CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP
          + " to " + flag);
    }
    useIpForTokenService = flag;
    hostResolver = !useIpForTokenService
        ? new QualifiedHostResolver(cachingInterval)
        : new StandardHostResolver(cachingInterval);
  }
  
  /**
   * TGS must have the server principal of the form "krbtgt/FOO@FOO".
   * @param principal
   * @return true or false
   */
  static boolean 
  isTGSPrincipal(KerberosPrincipal principal) {
    if (principal == null)
      return false;
    if (principal.getName().equals("krbtgt/" + principal.getRealm() + 
        "@" + principal.getRealm())) {
      return true;
    }
    return false;
  }
  
  /**
   * Check whether the server principal is the TGS's principal
   * @param ticket the original TGT (the ticket that is obtained when a 
   * kinit is done)
   * @return true or false
   */
  protected static boolean isOriginalTGT(KerberosTicket ticket) {
    return isTGSPrincipal(ticket.getServer());
  }

  /**
   * Convert Kerberos principal name pattern to valid Kerberos principal
   * names. It replaces hostname pattern with hostname, which should be
   * fully-qualified domain name. If hostname is null or "0.0.0.0", it uses
   * dynamically looked-up fqdn of the current host instead.
   * 
   * @param principalConfig
   *          the Kerberos principal name conf value to convert
   * @param hostname
   *          the fully-qualified domain name used for substitution
   * @return converted Kerberos principal name
   * @throws IOException if the client address cannot be determined
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static String getServerPrincipal(String principalConfig,
      String hostname) throws IOException {
    String[] components = getComponents(principalConfig);
    if (components == null || components.length != 3
        || !components[1].equals(HOSTNAME_PATTERN)) {
      return principalConfig;
    } else {
      return replacePattern(components, hostname);
    }
  }
  
  /**
   * Convert Kerberos principal name pattern to valid Kerberos principal names.
   * This method is similar to {@link #getServerPrincipal(String, String)},
   * except 1) the reverse DNS lookup from addr to hostname is done only when
   * necessary, 2) param addr can't be null (no default behavior of using local
   * hostname when addr is null).
   * 
   * @param principalConfig
   *          Kerberos principal name pattern to convert
   * @param addr
   *          InetAddress of the host used for substitution
   * @return converted Kerberos principal name
   * @throws IOException if the client address cannot be determined
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static String getServerPrincipal(String principalConfig,
      InetAddress addr) throws IOException {
    String[] components = getComponents(principalConfig);
    if (components == null || components.length != 3
        || !components[1].equals(HOSTNAME_PATTERN)) {
      return principalConfig;
    } else {
      if (addr == null) {
        throw new IOException("Can't replace " + HOSTNAME_PATTERN
            + " pattern since client address is null");
      }
      return replacePattern(components, domainNameResolver.getHostnameByIP(addr));
    }
  }
  
  private static String[] getComponents(String principalConfig) {
    if (principalConfig == null)
      return null;
    return principalConfig.split("[/@]");
  }
  
  private static String replacePattern(String[] components, String hostname)
      throws IOException {
    String fqdn = hostname;
    if (fqdn == null || fqdn.isEmpty() || fqdn.equals("0.0.0.0")) {
      fqdn = getLocalHostName(null);
    }
    return components[0] + "/" +
        StringUtils.toLowerCase(fqdn) + "@" + components[2];
  }

  /**
   * Retrieve the name of the current host. Multihomed hosts may restrict the
   * hostname lookup to a specific interface and nameserver with {@link
   * org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_SECURITY_DNS_INTERFACE_KEY}
   * and {@link org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_SECURITY_DNS_NAMESERVER_KEY}
   *
   * @param conf Configuration object. May be null.
   * @return
   * @throws UnknownHostException
   */
  static String getLocalHostName(@Nullable Configuration conf)
      throws UnknownHostException {
    if (conf != null) {
      String dnsInterface = conf.get(HADOOP_SECURITY_DNS_INTERFACE_KEY);
      String nameServer = conf.get(HADOOP_SECURITY_DNS_NAMESERVER_KEY);

      if (dnsInterface != null) {
        return DNS.getDefaultHost(dnsInterface, nameServer, true);
      } else if (nameServer != null) {
        throw new IllegalArgumentException(HADOOP_SECURITY_DNS_NAMESERVER_KEY +
            " requires " + HADOOP_SECURITY_DNS_INTERFACE_KEY + ". Check your" +
            "configuration.");
      }
    }

    // Fallback to querying the default hostname as we did before.
    return InetAddress.getLocalHost().getCanonicalHostName();
  }

  /**
   * Login as a principal specified in config. Substitute $host in
   * user's Kerberos principal name with a dynamically looked-up fully-qualified
   * domain name of the current host.
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void login(final Configuration conf,
      final String keytabFileKey, final String userNameKey) throws IOException {
    login(conf, keytabFileKey, userNameKey, getLocalHostName(conf));
  }

  /**
   * Login as a principal specified in config. Substitute $host in user's Kerberos principal 
   * name with hostname. If non-secure mode - return. If no keytab available -
   * bail out with an exception
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @param hostname
   *          hostname to use for substitution
   * @throws IOException if the config doesn't specify a keytab
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void login(final Configuration conf,
      final String keytabFileKey, final String userNameKey, String hostname)
      throws IOException {
    
    if(! UserGroupInformation.isSecurityEnabled()) 
      return;
    
    String keytabFilename = conf.get(keytabFileKey);
    if (keytabFilename == null || keytabFilename.length() == 0) {
      throw new IOException(
          "Running in secure mode, but config doesn't have a keytab for key: " + keytabFileKey);
    }

    String principalConfig = conf.get(userNameKey, System
        .getProperty("user.name"));
    String principalName = SecurityUtil.getServerPrincipal(principalConfig,
        hostname);
    UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename);
  }

  /**
   * create the service name for a Delegation token
   * @param uri of the service
   * @param defPort is used if the uri lacks a port
   * @return the token service, or null if no authority
   * @see #buildTokenService(InetSocketAddress)
   */
  public static String buildDTServiceName(URI uri, int defPort) {
    String authority = uri.getAuthority();
    if (authority == null) {
      return null;
    }
    InetSocketAddress addr = NetUtils.createSocketAddr(authority, defPort);
    return buildTokenService(addr).toString();
   }
  
  /**
   * Get the host name from the principal name of format {@literal <}service
   * {@literal >}/host@realm.
   * @param principalName principal name of format as described above
   * @return host name if the the string conforms to the above format, else null
   */
  public static String getHostFromPrincipal(String principalName) {
    return new HadoopKerberosName(principalName).getHostName();
  }

  private static ServiceLoader<SecurityInfo> securityInfoProviders = 
    ServiceLoader.load(SecurityInfo.class);
  private static SecurityInfo[] testProviders = new SecurityInfo[0];

  /**
   * Test setup method to register additional providers.
   * @param providers a list of high priority providers to use
   */
  @InterfaceAudience.Private
  public static void setSecurityInfoProviders(SecurityInfo... providers) {
    testProviders = providers;
  }
  
  /**
   * Look up the KerberosInfo for a given protocol. It searches all known
   * SecurityInfo providers.
   * @param protocol the protocol class to get the information for
   * @param conf configuration object
   * @return the KerberosInfo or null if it has no KerberosInfo defined
   */
  public static KerberosInfo 
  getKerberosInfo(Class<?> protocol, Configuration conf) {
    for(SecurityInfo provider: testProviders) {
      KerberosInfo result = provider.getKerberosInfo(protocol, conf);
      if (result != null) {
        return result;
      }
    }
    
    synchronized (securityInfoProviders) {
      for(SecurityInfo provider: securityInfoProviders) {
        KerberosInfo result = provider.getKerberosInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      }
    }
    return null;
  }

  /**
   * Look up the client principal for a given protocol. It searches all known
   * SecurityInfo providers.
   * @param protocol the protocol class to get the information for
   * @param conf configuration object
   * @return client principal or null if it has no client principal defined.
   */
  public static String getClientPrincipal(Class<?> protocol,
      Configuration conf) {
    String user = null;
    KerberosInfo krbInfo = SecurityUtil.getKerberosInfo(protocol, conf);
    if (krbInfo != null) {
      String key = krbInfo.clientPrincipal();
      user = (key != null && !key.isEmpty()) ? conf.get(key) : null;
    }
    return user;
  }

  /**
   * Look up the TokenInfo for a given protocol. It searches all known
   * SecurityInfo providers.
   * @param protocol The protocol class to get the information for.
   * @param conf Configuration object
   * @return the TokenInfo or null if it has no KerberosInfo defined
   */
  public static TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    for(SecurityInfo provider: testProviders) {
      TokenInfo result = provider.getTokenInfo(protocol, conf);
      if (result != null) {
        return result;
      }      
    }
    
    synchronized (securityInfoProviders) {
      for(SecurityInfo provider: securityInfoProviders) {
        TokenInfo result = provider.getTokenInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      } 
    }
    
    return null;
  }

  /**
   * Decode the given token's service field into an InetAddress
   * @param token from which to obtain the service
   * @return InetAddress for the service
   */
  public static InetSocketAddress getTokenServiceAddr(Token<?> token) {
    return NetUtils.createSocketAddr(token.getService().toString());
  }

  /**
   * Set the given token's service to the format expected by the RPC client 
   * @param token a delegation token
   * @param addr the socket for the rpc connection
   */
  public static void setTokenService(Token<?> token, InetSocketAddress addr) {
    Text service = buildTokenService(addr);
    if (token != null) {
      token.setService(service);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Acquired token "+token);  // Token#toString() prints service
      }
    } else {
      LOG.warn("Failed to get token for service "+service);
    }
  }
  
  /**
   * Construct the service key for a token
   * @param addr InetSocketAddress of remote connection with a token
   * @return "ip:port" or "host:port" depending on the value of
   *          hadoop.security.token.service.use_ip
   */
  public static Text buildTokenService(InetSocketAddress addr) {
    String host = null;
    if (useIpForTokenService) {
      if (addr.isUnresolved()) { // host has no ip address
        throw new IllegalArgumentException(
            new UnknownHostException(addr.getHostName())
        );
      }
      host = addr.getAddress().getHostAddress();
    } else {
      host = StringUtils.toLowerCase(addr.getHostName());
    }
    return new Text(host + ":" + addr.getPort());
  }

  /**
   * Construct the service key for a token
   * @param uri of remote connection with a token
   * @return "ip:port" or "host:port" depending on the value of
   *          hadoop.security.token.service.use_ip
   */
  public static Text buildTokenService(URI uri) {
    return buildTokenService(NetUtils.createSocketAddr(uri.getAuthority()));
  }
  
  /**
   * Perform the given action as the daemon's login user. If the login
   * user cannot be determined, this will log a FATAL error and exit
   * the whole JVM.
   *
   * @param action action.
   * @param <T> generic type T.
   * @return generic type T.
   */
  public static <T> T doAsLoginUserOrFatal(PrivilegedAction<T> action) { 
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation ugi = null;
      try { 
        ugi = UserGroupInformation.getLoginUser();
      } catch (IOException e) {
        LOG.error("Exception while getting login user", e);
        e.printStackTrace();
        Runtime.getRuntime().exit(-1);
      }
      return ugi.doAs(action);
    } else {
      return action.run();
    }
  }
  
  /**
   * Perform the given action as the daemon's login user. If an
   * InterruptedException is thrown, it is converted to an IOException.
   *
   * @param action the action to perform
   * @param <T> Generics Type T.
   * @return the result of the action
   * @throws IOException in the event of error
   */
  public static <T> T doAsLoginUser(PrivilegedExceptionAction<T> action)
      throws IOException {
    return doAsUser(UserGroupInformation.getLoginUser(), action);
  }

  /**
   * Perform the given action as the daemon's current user. If an
   * InterruptedException is thrown, it is converted to an IOException.
   *
   * @param action the action to perform
   * @param <T> generic type T.
   * @return the result of the action
   * @throws IOException in the event of error
   */
  public static <T> T doAsCurrentUser(PrivilegedExceptionAction<T> action)
      throws IOException {
    return doAsUser(UserGroupInformation.getCurrentUser(), action);
  }

  private static <T> T doAsUser(UserGroupInformation ugi,
      PrivilegedExceptionAction<T> action) throws IOException {
    try {
      return ugi.doAs(action);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Resolves a host subject to the security requirements determined by
   * hadoop.security.token.service.use_ip. Optionally logs slow resolutions.
   * 
   * @param hostname host or ip to resolve
   * @return a resolved host
   * @throws UnknownHostException if the host doesn't exist
   */
  @InterfaceAudience.Private
  public static
  InetAddress getByName(String hostname) throws UnknownHostException {
    if (logSlowLookups || LOG.isTraceEnabled()) {
      StopWatch lookupTimer = new StopWatch().start();
      InetAddress result = hostResolver.getByName(hostname);
      long elapsedMs = lookupTimer.stop().now(TimeUnit.MILLISECONDS);

      if (elapsedMs >= slowLookupThresholdMs) {
        LOG.warn("Slow name lookup for " + hostname + ". Took " + elapsedMs +
            " ms.");
      } else if (LOG.isTraceEnabled()) {
        LOG.trace("Name lookup for " + hostname + " took " + elapsedMs +
            " ms.");
      }
      return result;
    } else {
      return hostResolver.getByName(hostname);
    }
  }

  interface HostResolver {
    InetAddress getByName(String host) throws UnknownHostException;
  }

  static abstract class CacheableHostResolver implements HostResolver {
    private volatile LoadingCache<String, InetAddress> cache;

    CacheableHostResolver(long expiryIntervalSecs) {
      if (expiryIntervalSecs > 0) {
        cache = CacheBuilder.newBuilder()
            .expireAfterWrite(expiryIntervalSecs, TimeUnit.SECONDS)
            .build(new CacheLoader<String, InetAddress>() {
              @Override
              public InetAddress load(String key) throws Exception {
                return resolve(key);
              }
            });
      }
    }
    protected abstract InetAddress resolve(String host) throws UnknownHostException;

    @Override
    public InetAddress getByName(String host) throws UnknownHostException {
      if (cache != null) {
        try {
          return cache.get(host);
        } catch (Exception e) {
          Throwable cause = e.getCause();
          if (cause instanceof UnknownHostException) {
            throw (UnknownHostException) cause;
          }
          String message = (cause != null ? cause.getMessage() : "Unknown error");
          throw new UnknownHostException("Error resolving host " + host + ": " + message);
        }
      } else {
        return resolve(host);
      }
    }

    @VisibleForTesting
    public LoadingCache<String, InetAddress> getCache() {
      return cache;
    }
  }
  /**
   * Uses standard java host resolution
   */
  static class StandardHostResolver extends CacheableHostResolver {

    StandardHostResolver(long expiryIntervalSecs) {
      super(expiryIntervalSecs);
    }

    @Override
    public InetAddress resolve(String host) throws UnknownHostException {
      return InetAddress.getByName(host);
    }
  }
  
  /**
   * This an alternate resolver with important properties that the standard
   * java resolver lacks:
   * 1) The hostname is fully qualified.  This avoids security issues if not
   *    all hosts in the cluster do not share the same search domains.  It
   *    also prevents other hosts from performing unnecessary dns searches.
   *    In contrast, InetAddress simply returns the host as given.
   * 2) The InetAddress is instantiated with an exact host and IP to prevent
   *    further unnecessary lookups.  InetAddress may perform an unnecessary
   *    reverse lookup for an IP.
   * 3) A call to getHostName() will always return the qualified hostname, or
   *    more importantly, the IP if instantiated with an IP.  This avoids
   *    unnecessary dns timeouts if the host is not resolvable.
   * 4) Point 3 also ensures that if the host is re-resolved, ex. during a
   *    connection re-attempt, that a reverse lookup to host and forward
   *    lookup to IP is not performed since the reverse/forward mappings may
   *    not always return the same IP.  If the client initiated a connection
   *    with an IP, then that IP is all that should ever be contacted.
   *    
   * NOTE: this resolver is only used if:
   *       hadoop.security.token.service.use_ip=false 
   */
  protected static class QualifiedHostResolver extends CacheableHostResolver {
    private List<String> searchDomains = new ArrayList<>();
    {
      ResolverConfig resolverConfig = ResolverConfig.getCurrentConfig();
      for (Name name : resolverConfig.searchPath()) {
        searchDomains.add(name.toString());
      }
    }

    QualifiedHostResolver() {
      this(0);
    }

    QualifiedHostResolver(long expiryIntervalSecs) {
      super(expiryIntervalSecs);
    }
    /**
     * Create an InetAddress with a fully qualified hostname of the given
     * hostname.  InetAddress does not qualify an incomplete hostname that
     * is resolved via the domain search list.
     * {@link InetAddress#getCanonicalHostName()} will fully qualify the
     * hostname, but it always return the A record whereas the given hostname
     * may be a CNAME.
     * 
     * @param host a hostname or ip address
     * @return InetAddress with the fully qualified hostname or ip
     * @throws UnknownHostException if host does not exist
     */
    @Override
    public InetAddress resolve(String host) throws UnknownHostException {
      InetAddress addr = null;

      if (InetAddresses.isInetAddress(host)) {
        // valid ip address. use it as-is
        addr = InetAddresses.forString(host);
        // set hostname
        addr = InetAddress.getByAddress(host, addr.getAddress());
      } else if (host.endsWith(".")) {
        // a rooted host ends with a dot, ex. "host."
        // rooted hosts never use the search path, so only try an exact lookup
        addr = getByExactName(host);
      } else if (host.contains(".")) {
        // the host contains a dot (domain), ex. "host.domain"
        // try an exact host lookup, then fallback to search list
        addr = getByExactName(host);
        if (addr == null) {
          addr = getByNameWithSearch(host);
        }
      } else {
        // it's a simple host with no dots, ex. "host"
        // try the search list, then fallback to exact host
        InetAddress loopback = InetAddress.getByName(null);
        if (host.equalsIgnoreCase(loopback.getHostName())) {
          addr = InetAddress.getByAddress(host, loopback.getAddress());
        } else {
          addr = getByNameWithSearch(host);
          if (addr == null) {
            addr = getByExactName(host);
          }
        }
      }
      // unresolvable!
      if (addr == null) {
        throw new UnknownHostException(host);
      }
      return addr;
    }

    InetAddress getByExactName(String host) {
      InetAddress addr = null;
      // InetAddress will use the search list unless the host is rooted
      // with a trailing dot.  The trailing dot will disable any use of the
      // search path in a lower level resolver.  See RFC 1535.
      String fqHost = host;
      if (!fqHost.endsWith(".")) fqHost += ".";
      try {
        addr = getInetAddressByName(fqHost);
        // can't leave the hostname as rooted or other parts of the system
        // malfunction, ex. kerberos principals are lacking proper host
        // equivalence for rooted/non-rooted hostnames
        addr = InetAddress.getByAddress(host, addr.getAddress());
      } catch (UnknownHostException e) {
        // ignore, caller will throw if necessary
      }
      return addr;
    }

    InetAddress getByNameWithSearch(String host) {
      InetAddress addr = null;
      if (host.endsWith(".")) { // already qualified?
        addr = getByExactName(host); 
      } else {
        for (String domain : searchDomains) {
          String dot = !domain.startsWith(".") ? "." : "";
          addr = getByExactName(host + dot + domain);
          if (addr != null) break;
        }
      }
      return addr;
    }

    // implemented as a separate method to facilitate unit testing
    InetAddress getInetAddressByName(String host) throws UnknownHostException {
      return InetAddress.getByName(host);
    }

    void setSearchDomains(String ... domains) {
      searchDomains = Arrays.asList(domains);
    }
  }

  public static AuthenticationMethod getAuthenticationMethod(Configuration conf) {
    String value = conf.get(HADOOP_SECURITY_AUTHENTICATION, "simple");
    try {
      return Enum.valueOf(AuthenticationMethod.class,
          StringUtils.toUpperCase(value));
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException("Invalid attribute value for " +
          HADOOP_SECURITY_AUTHENTICATION + " of " + value);
    }
  }

  public static void setAuthenticationMethod(
      AuthenticationMethod authenticationMethod, Configuration conf) {
    if (authenticationMethod == null) {
      authenticationMethod = AuthenticationMethod.SIMPLE;
    }
    conf.set(HADOOP_SECURITY_AUTHENTICATION,
        StringUtils.toLowerCase(authenticationMethod.toString()));
  }

  /*
   * Check if a given port is privileged.
   * The ports with number smaller than 1024 are treated as privileged ports in
   * unix/linux system. For other operating systems, use this method with care.
   * For example, Windows doesn't have the concept of privileged ports.
   * However, it may be used at Windows client to check port of linux server.
   * 
   * @param port the port number
   * @return true for privileged ports, false otherwise
   * 
   */
  public static boolean isPrivilegedPort(final int port) {
    return port < 1024;
  }

  /**
   * Utility method to fetch ZK auth info from the configuration.
   *
   * @param conf configuration.
   * @param configKey config key.
   * @throws java.io.IOException if the Zookeeper ACLs configuration file
   * cannot be read
   * @throws ZKUtil.BadAuthFormatException if the auth format is invalid
   * @return ZKAuthInfo List.
   */
  public static List<ZKUtil.ZKAuthInfo> getZKAuthInfos(Configuration conf,
      String configKey) throws IOException {
    char[] zkAuthChars = conf.getPassword(configKey);
    String zkAuthConf =
        zkAuthChars != null ? String.valueOf(zkAuthChars) : null;
    try {
      zkAuthConf = ZKUtil.resolveConfIndirection(zkAuthConf);
      if (zkAuthConf != null) {
        return ZKUtil.parseAuth(zkAuthConf);
      } else {
        return Collections.emptyList();
      }
    } catch (IOException | ZKUtil.BadAuthFormatException e) {
      LOG.error("Couldn't read Auth based on {}", configKey);
      throw e;
    }
  }

  public static void validateSslConfiguration(TruststoreKeystore truststoreKeystore)
          throws ConfigurationException {
    if (org.apache.commons.lang3.StringUtils.isEmpty(truststoreKeystore.keystoreLocation)) {
      throw new ConfigurationException(
          "The keystore location parameter is empty for the ZooKeeper client connection.");
    }
    if (org.apache.commons.lang3.StringUtils.isEmpty(truststoreKeystore.keystorePassword)) {
      throw new ConfigurationException(
          "The keystore password parameter is empty for the ZooKeeper client connection.");
    }
    if (org.apache.commons.lang3.StringUtils.isEmpty(truststoreKeystore.truststoreLocation)) {
      throw new ConfigurationException(
          "The truststore location parameter is empty for the ZooKeeper client connection.");
    }
    if (org.apache.commons.lang3.StringUtils.isEmpty(truststoreKeystore.truststorePassword)) {
      throw new ConfigurationException(
          "The truststore password parameter is empty for the ZooKeeper client connection.");
    }
  }

  /**
   * Configure ZooKeeper Client with SSL/TLS connection.
   * @param zkClientConfig ZooKeeper Client configuration
   * @param truststoreKeystore truststore keystore, that we use to set the SSL configurations
   * @throws ConfigurationException if the SSL configs are empty
   */
  public static void setSslConfiguration(ZKClientConfig zkClientConfig,
                                         TruststoreKeystore truststoreKeystore)
          throws ConfigurationException {
    setSslConfiguration(zkClientConfig, truststoreKeystore, new ClientX509Util());
  }

  public static void setSslConfiguration(ZKClientConfig zkClientConfig,
                                         TruststoreKeystore truststoreKeystore,
                                         ClientX509Util x509Util)
          throws ConfigurationException {
    validateSslConfiguration(truststoreKeystore);
    LOG.info("Configuring the ZooKeeper client to use SSL/TLS encryption for connecting to the "
        + "ZooKeeper server.");
    LOG.debug("Configuring the ZooKeeper client with {} location: {}.",
        truststoreKeystore.keystoreLocation,
        CommonConfigurationKeys.ZK_SSL_KEYSTORE_LOCATION);
    LOG.debug("Configuring the ZooKeeper client with {} location: {}.",
        truststoreKeystore.truststoreLocation,
        CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_LOCATION);

    zkClientConfig.setProperty(ZKClientConfig.SECURE_CLIENT, "true");
    zkClientConfig.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET,
        "org.apache.zookeeper.ClientCnxnSocketNetty");
    zkClientConfig.setProperty(x509Util.getSslKeystoreLocationProperty(),
        truststoreKeystore.keystoreLocation);
    zkClientConfig.setProperty(x509Util.getSslKeystorePasswdProperty(),
        truststoreKeystore.keystorePassword);
    zkClientConfig.setProperty(x509Util.getSslTruststoreLocationProperty(),
        truststoreKeystore.truststoreLocation);
    zkClientConfig.setProperty(x509Util.getSslTruststorePasswdProperty(),
        truststoreKeystore.truststorePassword);
  }

  /**
   * Helper class to contain the Truststore/Keystore paths for the ZK client connection over
   * SSL/TLS.
   */
  public static class TruststoreKeystore {
    private final String keystoreLocation;
    private final String keystorePassword;
    private final String truststoreLocation;
    private final String truststorePassword;

    /**
     * Configuration for the ZooKeeper connection when SSL/TLS is enabled.
     * When a value is not configured, ensure that empty string is set instead of null.
     *
     * @param conf ZooKeeper Client configuration
     */
    public TruststoreKeystore(Configuration conf) {
      keystoreLocation = conf.get(CommonConfigurationKeys.ZK_SSL_KEYSTORE_LOCATION, "");
      keystorePassword = conf.get(CommonConfigurationKeys.ZK_SSL_KEYSTORE_PASSWORD, "");
      truststoreLocation = conf.get(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_LOCATION, "");
      truststorePassword = conf.get(CommonConfigurationKeys.ZK_SSL_TRUSTSTORE_PASSWORD, "");
    }

    public String getKeystoreLocation() {
      return keystoreLocation;
    }

    public String getKeystorePassword() {
      return keystorePassword;
    }

    public String getTruststoreLocation() {
      return truststoreLocation;
    }

    public String getTruststorePassword() {
      return truststorePassword;
    }
  }
}
