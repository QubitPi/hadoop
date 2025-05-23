/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import DS from 'ember-data';
import {PARTITION_LABEL} from '../../constants';

export default DS.JSONAPISerializer.extend({

    normalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      var children = [];
      if (payload.queues && payload.queues.queue) {
        payload.queues.queue.forEach(function(queue) {
          children.push(queue.queuePath);
        });
      }

      var includedData = [];
      var relationshipUserData = [];

      // update user models
      if (payload.users && payload.users.user) {
        payload.users.user.forEach(function(u) {
          var defaultPartitionResource = u.resources.resourceUsagesByPartition[0];
          var maxAMResource = defaultPartitionResource.amLimit;
          includedData.push({
            type: "YarnUser",
            id: u.username + "_" + payload.queuePath,
            attributes: {
              name: u.username,
              queueName: payload.queuePath,
              usedMemoryMB: u.resourcesUsed.memory || 0,
              usedVCore: u.resourcesUsed.vCores || 0,
              maxMemoryMB: u.userResourceLimit.memory || 0,
              maxVCore: u.userResourceLimit.vCores || 0,
              amUsedMemoryMB: u.AMResourceUsed.memory || 0,
              amUsedVCore: u.AMResourceUsed.vCores || 0,
              maxAMMemoryMB: maxAMResource.memory || 0,
              maxAMVCore: maxAMResource.vCores || 0,
              userWeight: u.userWeight || '',
              activeApps: u.numActiveApplications || 0,
              pendingApps: u.numPendingApplications || 0
            }
          });

          relationshipUserData.push({
            type: "YarnUser",
            id: u.username + "_" + payload.queuePath,
          });
        });
      }

      var partitions = [];
      var partitionMap = {};
      if ("capacities" in payload) {
        partitions = payload.capacities.queueCapacitiesByPartition.map(
          cap => cap.partitionName || PARTITION_LABEL);
        partitionMap = payload.capacities.queueCapacitiesByPartition.reduce((init, cap) => {
          init[cap.partitionName || PARTITION_LABEL] = cap;
          return init;
        }, {});
      } else {
        partitions = [PARTITION_LABEL];
        partitionMap[PARTITION_LABEL] = {
          partitionName: "",
          capacity: payload.capacity,
          maxCapacity: payload.maxCapacity,
          usedCapacity: payload.usedCapacity,
          absoluteCapacity: 'absoluteCapacity' in payload ? payload.absoluteCapacity : payload.capacity,
          absoluteMaxCapacity: 'absoluteMaxCapacity' in payload ? payload.absoluteMaxCapacity : payload.maxCapacity,
          absoluteUsedCapacity: 'absoluteUsedCapacity' in payload ? payload.absoluteUsedCapacity : payload.usedCapacity
        };
      }

      //add here the partitioninfo
      ///scheduler/schedulerInfo/queues/queue[2]/resources/resourceUsagesByPartition/partitionName
      //resources.resourceUsagesByPartition[].used.memory
      var resourcePartitions = [];
      var resourceUsagesByPartitionMap = {};
      if ("resources" in payload){
        resourcePartitions = payload.resources.resourceUsagesByPartition.map(
          res => res.partitionName || PARTITION_LABEL);
          resourceUsagesByPartitionMap = payload.resources.resourceUsagesByPartition.reduce((init, res) => {
          init[res.partitionName || PARTITION_LABEL] = res;
          return init;
        }, {});
      }else{
        resourceUsagesByPartitionMap[PARTITION_LABEL] = {
          partitionName: ""
        };
      }

      var fixedPayload = {
        id: id,
        type: primaryModelClass.modelName, // yarn-queue
        attributes: {
          name: payload.queueName,
          queuePath: payload.queuePath,
          parent: payload.myParent,
          children: children,
          capacity: payload.capacity,
          usedCapacity: payload.usedCapacity,
          maxCapacity: payload.maxCapacity,
          absCapacity: payload.absoluteCapacity,
          absMaxCapacity: payload.absoluteMaxCapacity,
          absUsedCapacity: payload.absoluteUsedCapacity,
          weight: payload.weight,
          normalizedWeight: payload.normalizedWeight,
          creationMethod: payload.creationMethod,
          state: payload.state,
          orderingPolicyInfo: payload.orderingPolicyInfo,
          userLimit: payload.userLimit,
          userLimitFactor: payload.userLimitFactor,
          preemptionDisabled: payload.preemptionDisabled,
          intraQueuePreemptionDisabled: payload.intraQueuePreemptionDisabled,
          numPendingApplications: payload.numPendingApplications,
          numActiveApplications: payload.numActiveApplications,
          numContainers: payload.numContainers,
          maxApplications: payload.maxApplications,
          maxApplicationsPerUser: payload.maxApplicationsPerUser,
          nodeLabels: payload.nodeLabels,
          defaultNodeLabelExpression: payload.defaultNodeLabelExpression,
          resources: payload.resources,
          defaultPriority: payload.defaultPriority,
          partitions: partitions,
          partitionMap: partitionMap,
          resourceUsagesByPartitionMap: resourceUsagesByPartitionMap,
          type: "capacity",
        },
        // Relationships
        relationships: {
          users: {
            data: relationshipUserData
          }
        }
      };
      return {
        queue: this._super(store, primaryModelClass, fixedPayload, id, requestType),
        includedData: includedData
      };
    },

    handleQueue(store, primaryModelClass, payload, id, requestType) {
      var data = [];
      var includedData = [];
      var result = this.normalizeSingleResponse(store, primaryModelClass,
        payload, id, requestType);

      data.push(result.queue);
      includedData = includedData.concat(result.includedData);

      if (payload.queues && payload.queues.queue) {
        for (var i = 0; i < payload.queues.queue.length; i++) {
          var queue = payload.queues.queue[i];
          queue.myParent = payload.queuePath;
          var childResult = this.handleQueue(store, primaryModelClass, queue,
            queue.queuePath,
            requestType);

          data = data.concat(childResult.data);
          includedData = includedData.concat(childResult.includedData);
        }
      }

      return {
        data: data,
        includedData: includedData
      };
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id, requestType) {
      var normalizedArrayResponse = {};
      var result = this.handleQueue(store, primaryModelClass,
        payload.scheduler.schedulerInfo, "root", requestType);

      normalizedArrayResponse.data = result.data;
      normalizedArrayResponse.included = result.includedData;

      return normalizedArrayResponse;
    }
});
