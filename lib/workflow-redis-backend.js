// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var util = require('util');
var async = require('async');
var Logger = require('bunyan');
var wf = require('wf');
var makeEmitter = wf.makeEmitter;
var sprintf = util.format;

var WorkflowRedisBackend = module.exports = function (config) {
    if (typeof (config) !== 'object') {
        throw new TypeError('\'config\' (Object) required');
    }
    var client;

    var log;

    if (config.log) {
        log = config.log.child({component: 'wf-redis-backend'});
    } else {
        if (!config.logger) {
            config.logger = {};
        }

        config.logger.name = 'wf-redis-backend';
        config.logger.serializers = {
            err: Logger.stdSerializers.err
        };

        config.logger.streams = config.logger.streams || [ {
            level: 'info',
            stream: process.stdout
        }];

        log = new Logger(config.logger);
    }

    var backend = {
        log: log,
        config: config,
        client: client,
        quit: function quit(callback) {
            if (client.connected === true) {
                return client.quit(callback);
            } else {
                return callback();
            }
        },
        connected: false
    };

    makeEmitter(backend);

    function init(callback) {
        var port = config.port || 6379;
        var host = config.host || '127.0.0.1';
        var db_num = config.db || 1;
        var redis = require('redis');


        if (config.debug) {
            redis.debug_mode = true;
        }

        client = redis.createClient(port, host, config);

        backend.client = client;

        if (config.password) {
            client.auth(config.password);
        }

        client.on('error', function (err) {
            log.error({err: err}, 'Redis client error');
            backend.connected = false;
            backend.emit('error', err);
        });

        client.on('connect', function () {
            client.select(db_num, function (err, res) {
                if (err) {
                    throw err;
                }
                backend.connected = true;
                backend.emit('connected');
                if (callback) {
                    return callback();
                }
            });
        });
    }

    backend.init = init;

    // workflow - Workflow object
    // meta - Any additional information to pass to the backend which is not
    //        workflow properties
    // cb - f(err, workflow)
    function createWorkflow(workflow, meta, cb) {
        if (typeof (meta) === 'function') {
            cb = meta;
            meta = {};
        }
        var multi = client.multi();
        var p;

        // TODO: A good place to verify that the same tasks are not on the chain
        // and into the onerror callback (GH-1).


        for (p in workflow) {
            if (typeof (workflow[p]) === 'object') {
                workflow[p] = JSON.stringify(workflow[p]);
            }
        }
        // Save the workflow as a Hash
        multi.hmset('workflow:' + workflow.uuid, workflow);
        // Add the name to the wf_workflow_names set in order to be able to
        // check with SISMEMBER wf_workflow_names workflow.name
        multi.sadd('wf_workflow_names', workflow.name);
        multi.sadd('wf_workflows', workflow.uuid);

        // Validate there is not another workflow with the same name
        client.sismember(
            'wf_workflow_names',
            workflow.name,
            function (err, result) {
                if (err) {
                    log.error({err: err});
                    return cb(new wf.BackendInternalError(err));
                }

                if (result === 1) {
                    return cb(new wf.BackendInvalidArgumentError(
                      'Workflow.name must be unique. A workflow with name "' +
                      workflow.name + '" already exists'));
                }

                // Really execute everything on a transaction:
                return multi.exec(function (err, replies) {
                    // console.log(replies); => [ 'OK', 0 ]
                    if (err) {
                        log.error({err: err});
                        return cb(new wf.BackendInternalError(err));
                    } else {
                        if (workflow.chain) {
                            try {
                                workflow.chain = JSON.parse(workflow.chain);
                            } catch (e1) {}
                        }
                        if (workflow.onerror) {
                            try {
                                workflow.onerror = JSON.parse(workflow.onerror);
                            } catch (e2) {}
                        }
                        return cb(null, workflow);
                    }
                });

          });
    }

    backend.createWorkflow = createWorkflow;

    // uuid - Workflow.uuid
    // meta - Any additional information to pass to the backend which is not
    //        workflow properties
    // callback - f(err, workflow)
    function getWorkflow(uuid, meta, callback) {
        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        client.hgetall('workflow:' + uuid, function (err, workflow) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            } else if (!workflow || Object.keys(workflow).length === 0) {
                return callback(new wf.BackendResourceNotFoundError(sprintf(
                  'Workflow with uuid \'%s\' does not exist', uuid)));
            } else {
                if (workflow.chain) {
                    workflow.chain = JSON.parse(workflow.chain);
                }
                if (workflow.onerror) {
                    workflow.onerror = JSON.parse(workflow.onerror);
                }
                if (workflow.timeout) {
                    workflow.timeout = Number(workflow.timeout);
                }
                return callback(null, workflow);
            }
        });
    }

    backend.getWorkflow = getWorkflow;

    // workflow - the workflow object
    // meta - Any additional information to pass to the backend which is not
    //        workflow properties
    // cb - f(err, boolean)
    function deleteWorkflow(workflow, meta, cb) {
        if (typeof (meta) === 'function') {
            cb = meta;
            meta = {};
        }

        var multi = client.multi();

        multi.del('workflow:' + workflow.uuid);
        multi.srem('wf_workflow_names', workflow.name);
        multi.srem('wf_workflows', workflow.uuid);
        multi.exec(function (err, replies) {
            // console.log(replies); => [ 1, 1 ]
            if (err) {
                log.error({err: err});
                return cb(new wf.BackendInternalError(err));
            } else {
                return cb(null, true);
            }
        });
    }

    backend.deleteWorkflow = deleteWorkflow;

    // workflow - update workflow object.
    // meta - Any additional information to pass to the backend which is not
    //        workflow properties
    // cb - f(err, workflow)
    function updateWorkflow(workflow, meta, cb) {
        if (typeof (meta) === 'function') {
            cb = meta;
            meta = {};
        }

        var multi = client.multi();
            // We will use this variable to set the original workflow values
            // before the update, to enforce name uniqueness
        var aWorkflow;
        var p;

        // TODO: A good place to verify that the same tasks are not on the chain
        // and into the onerror callback (GH-1).


        for (p in workflow) {
            if (typeof (workflow[p]) === 'object') {
                workflow[p] = JSON.stringify(workflow[p]);
            }
        }
        // Save the workflow as a Hash
        multi.hmset('workflow:' + workflow.uuid, workflow);

        return client.exists(
          'workflow:' + workflow.uuid,
          function (err, result) {
            if (err) {
                log.error({err: err});
                return cb(new wf.BackendInternalError(err));
            }

            if (result === 0) {
                return cb(new wf.BackendResourceNotFoundError(
                  'Workflow does not exist. Cannot Update.'));
            }

            return getWorkflow(workflow.uuid, function (err, result) {
                if (err) {
                    log.error({err: err});
                    return cb(new wf.BackendInternalError(err));
                }
                aWorkflow = result;
                return client.sismember(
                  'wf_workflow_names',
                  workflow.name,
                  function (err, result) {
                    if (err) {
                        log.error({err: err});
                        return cb(new wf.BackendInternalError(err));
                    }

                    if (result === 1 && aWorkflow.name !== workflow.name) {
                        return cb(new wf.BackendInvalidArgumentError(
                          'Workflow.name must be unique. A workflow with ' +
                          'name "' + workflow.name + '" already exists'));
                    }

                    if (aWorkflow.name !== workflow.name) {
                        // Remove previous name, add the new one:
                        multi.srem('wf_workflow_names', aWorkflow.name);
                        multi.sadd('wf_workflow_names', workflow.name);
                    }

                    // Really execute everything on a transaction:
                    return multi.exec(function (err, replies) {
                        // console.log(replies); => [ 'OK', 0 ]
                        if (err) {
                            log.error({err: err});
                            return cb(new wf.BackendInternalError(err));
                        } else {
                            if (workflow.chain) {
                                workflow.chain = JSON.parse(workflow.chain);
                            }
                            if (workflow.onerror) {
                                workflow.onerror = JSON.parse(workflow.onerror);
                            }
                            return cb(null, workflow);
                        }
                    });

                  });
              });
          });

    }

    backend.updateWorkflow = updateWorkflow;

    // Return all the JSON.stringified job properties decoded back to objects
    // - job - (object) raw job from redis to decode
    // - callback - (function) f(job)
    function _decodeJob(job, callback) {
        if (job.chain) {
            try {
                job.chain = JSON.parse(job.chain);
            } catch (e1) {}
        }
        if (job.onerror) {
            try {
                job.onerror = JSON.parse(job.onerror);
            } catch (e2) {}
        }
        if (job.chain_results) {
            try {
                job.chain_results = JSON.parse(job.chain_results);
            } catch (e3) {}
        }
        if (job.onerror_results) {
            try {
                job.onerror_results = JSON.parse(job.onerror_results);
            } catch (e4) {}
        }
        if (job.params) {
            try {
                job.params = JSON.parse(job.params);
            } catch (e5) {}
        }
        // exec_after when saved as a timestamp comes back from Redis as a string
        // and cannot be parsed properlly by Date object
        if (isNaN(new Date(job.exec_after).getTime())) {
            job.exec_after = Number(job.exec_after);
        }
        if (isNaN(new Date(job.created_at).getTime())) {
            job.created_at = Number(job.created_at);
        }
        return callback(job);
    }

    // job - Job object
    // meta - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err, job)
    function createJob(job, meta, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowRedisBackend.createJob job(Object) required'));
        }

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        var multi = client.multi();
        var p;

        for (p in job) {
            if (typeof (job[p]) === 'object') {
                job[p] = JSON.stringify(job[p]);
            }
        }
        job.created_at = (job.created_at) ?
            new Date(job.created_at).getTime() : new Date().getTime();
        job.exec_after = (job.exec_after) ?
            new Date(job.exec_after).getTime() : new Date().getTime();

        // Save the job as a Hash
        multi.hmset('job:' + job.uuid, job);
        // Add the uuid to the wf_queued_jobs set in order to be able to use
        // it when we're about to run queued jobs
        multi.rpush('wf_queued_jobs', job.uuid);
        multi.sadd('wf_jobs', job.uuid);
        // If the job has a target, save into 'wf_target:target' to make
        // possible validation of duplicated jobs with same target:
        multi.sadd('wf_target:' + job.target, job.uuid);
        // Execute everything on a transaction:
        return multi.exec(function (err, replies) {
          // console.log(replies, false, 8); => [ 'OK', 1, 1 ]
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            } else {
                return _decodeJob(job, function (job) {
                    return callback(null, job);
                });
            }
        });
    }

    backend.createJob = createJob;


    // uuid - Job.uuid
    // meta - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err, job)
    function getJob(uuid, meta, callback) {
        if (typeof (uuid) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowRedisBackend.getJob uuid(String) required'));
        }

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        return client.hgetall('job:' + uuid, function (err, job) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            } else {
                if (Object.keys(job).length === 0) {
                    return callback(new wf.BackendResourceNotFoundError(sprintf(
                      'Job with uuid \'%s\' does not exist', uuid)));
                } else {
                    return _decodeJob(job, function (job) {
                        return callback(null, job);
                    });
                }
            }
        });
    }

    backend.getJob = getJob;


    // Get a single job property
    // uuid - Job uuid.
    // prop - (String) property name
    // meta - Any additional information to pass to moray which is not
    //        job properties
    // cb - callback f(err, value)
    function getJobProperty(uuid, prop, meta, cb) {
        if (typeof (uuid) === 'undefined') {
            return cb(new wf.BackendInternalError(
                  'WorkflowRedisBackend.getJobProperty uuid(String) required'));
        }

        if (typeof (prop) === 'undefined') {
            return cb(new wf.BackendInternalError(
                  'WorkflowRedisBackend.getJobProperty prop(String) required'));
        }

        if (typeof (meta) === 'function') {
            cb = meta;
            meta = {};
        }

        var encoded_props = ['chain', 'chain_results',
            'onerror', 'onerror_results', 'params'];

        return client.hget('job:' + uuid, prop, function (err, val) {
            if (err) {
                log.error({err: err});
                return cb(new wf.BackendInternalError(err));
            } else {
                if (encoded_props.indexOf(prop) !== -1) {
                    var v = val;
                    try {
                        v = JSON.parse(val);
                    } catch (e1) {}
                    return cb(null, v);
                } else {
                    return cb(null, val);
                }
            }
        });
    }

    backend.getJobProperty = getJobProperty;

    // job - the job object
    // callback - f(err) called with error in case there is a duplicated
    // job with the same target and same params
    function validateJobTarget(job, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
              'WorkflowRedisBackend.validateJobTarget job(Object) required'));
        }

        // If no target is given, we don't care:
        if (!job.target) {
            return callback(null);
        }

        return client.smembers(
            'wf_target:' + job.target,
            function (err, members) {
                if (err) {
                    log.error({err: err});
                    return callback(new wf.BackendInternalError(err));
                }
                if (members.length === 0) {
                    return callback(null);
                }
                // We have an array of jobs uuids with the same target. Need to
                // verify none of them has the same parameters than the job
                // we're trying to queue:
                // (NOTE: Make the limit of concurrent connections to Redis
                // configurable)
                return async.forEachLimit(members, 10, function (uuid, cb) {
                    getJob(uuid, function (err, aJob) {
                        if (err) {
                            cb(err);
                        } else {
                            if (aJob.workflow_uuid === job.workflow_uuid &&
                              JSON.stringify(aJob.params) ===
                              JSON.stringify(job.params)) {
                                // Already got same target, now also same
                                // workflow and same params fail it
                                cb(new wf.BackendInvalidArgumentError(
                                  'Another job with the same target' +
                                  ' and params is already queued'));
                            } else {
                                cb();
                            }
                        }
                    });
                }, function (err) {
                    if (err) {
                        log.error({err: err});
                        return callback(err);
                    }
                    return callback(null);
              });
          });
    }

    backend.validateJobTarget = validateJobTarget;

    // Get the next queued job.
    // index - Integer, optional. When given, it'll get the job at index
    //         position
    //         (when not given, it'll return the job at position zero).
    // callback - f(err, job)
    function nextJob(index, callback) {
        var slice = null;

        if (typeof (index) === 'function') {
            callback = index;
            index = 0;
        }

        return client.sort('wf_queued_jobs',
            'by', 'job:*->created_at', 'asc',
            'get', 'job:*->uuid',
            function (err, res) {
                if (err) {
                    log.error({err: err});
                    return callback(new wf.BackendInternalError(err));
                }

                if (res.length === 0) {
                    return callback(null, null);
                }

                slice = res.slice(index, index + 1);

                if (slice.length === 0) {
                    return callback(null, null);
                } else {
                    return getJob(slice[0], callback);
                }
            });
    }

    backend.nextJob = nextJob;


    // Lock a job, mark it as running by the given runner, update job status.
    // uuid - the job uuid (String)
    // runner_id - the runner identifier (String)
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function runJob(uuid, runner_id, callback) {
        if (typeof (uuid) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowRedisBackend.runJob uuid(String) required'));
        }
        var multi = client.multi();

        return client.lrem('wf_queued_jobs', 0, uuid, function (err, res) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            }

            if (res <= 0) {
                return callback(new wf.BackendPreconditionFailedError(
                  'Only queued jobs can be run'));
            }

            client.watch('job:' + uuid);
            multi.sadd('wf_runner:' + runner_id, uuid);
            multi.rpush('wf_running_jobs', uuid);
            multi.hset('job:' + uuid, 'execution', 'running');
            multi.hset('job:' + uuid, 'runner_id', runner_id);
            return multi.exec(function (err, replies) {
                if (err) {
                    log.error({err: err});
                    return callback(new wf.BackendInternalError(err));
                } else {
                    return getJob(uuid, callback);
                }
            });
        });
    }

    backend.runJob = runJob;

    // Unlock the job, mark it as finished, update the status, add the results
    // for every job's task.
    // job - the job object. It'll be saved to the backend with the provided
    //       properties.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function finishJob(job, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowRedisBackend.finishJob job(Object) required'));
        }

        var multi = client.multi();
        var p;

        for (p in job) {
            if (typeof (job[p]) === 'object') {
                job[p] = JSON.stringify(job[p]);
            }
        }

        return client.lrem('wf_running_jobs', 0, job.uuid,
            function (err, res) {
                if (err) {
                    log.error({err: err});
                    return callback(new wf.BackendInternalError(err));
                }

                if (res <= 0) {
                    return callback(new wf.BackendPreconditionFailedError(
                      'Only running jobs can be finished'));
                }
                if (job.execution === 'running') {
                    job.execution = 'succeeded';
                }

                multi.srem('wf_runner:' + job.runner_id, job.uuid);
                if (job.execution === 'succeeded') {
                    multi.rpush('wf_succeeded_jobs', job.uuid);
                } else if (job.execution === 'canceled') {
                    multi.rpush('wf_canceled_jobs', job.uuid);
                } else if (job.execution === 'retried') {
                    multi.rpush('wf_retried_jobs', job.uuid);
                } else {
                    multi.rpush('wf_failed_jobs', job.uuid);
                }

                multi.hmset('job:' + job.uuid, job);
                multi.hdel('job:' + job.uuid, 'runner_id');
                return multi.exec(function (err, replies) {
                    if (err) {
                        log.error({err: err});
                        return callback(new wf.BackendInternalError(err));
                    } else {
                        return getJob(job.uuid, callback);
                    }
                });
            });
    }

    backend.finishJob = finishJob;

    // Update the job while it is running with information regarding progress
    // job - the job object. It'll be saved to the backend with the provided
    //       properties.
    // meta - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function updateJob(job, meta, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowRedisBackend.updateJob job(Object) required'));
        }

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        var p;

        for (p in job) {
            if (typeof (job[p]) === 'object') {
                job[p] = JSON.stringify(job[p]);
            }
        }

        if (typeof (job.created_at) === 'string') {
            job.created_at = String(new Date(job.created_at).getTime());
        }
        if (typeof (job.exec_after) === 'string') {
            job.exec_after = String(new Date(job.exec_after).getTime());
        }
        if (typeof (job.exec_after) === 'number') {
            job.exec_after = String(job.exec_after);
        }

        return client.hmset('job:' + job.uuid, job, function (err, res) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            }
            return getJob(job.uuid, callback);
        });
    }

    backend.updateJob = updateJob;

    // Update only the given Job property. Intendeed to prevent conflicts with
    // two sources updating the same job at the same time, but different
    // properties
    // uuid - the job's uuid
    // prop - the name of the property to update
    // val - value to assign to such property
    // meta - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err) called with error if something fails, otherwise with
    // null.
    function updateJobProperty(uuid, prop, val, meta, callback) {
        if (typeof (uuid) === 'undefined') {
            return callback(new wf.BackendInternalError(
              'WorkflowRedisBackend.updateJobProperty uuid(String) required'));
        }

        if (typeof (prop) === 'undefined') {
            return callback(new wf.BackendInternalError(
              'WorkflowRedisBackend.updateJobProperty prop(String) required'));
        }

        if (typeof (val) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowRedisBackend.updateJobProperty val required'));
        }

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        if (typeof (val) === 'object') {
            val = JSON.stringify(val);
        }

        if (prop === 'created_at' && typeof (val) === 'string') {
            val = String(new Date(val).getTime());
        }

        if (prop === 'exec_after' && typeof (val) === 'string') {
            val = String(new Date(val).getTime());
        }

        return client.hset('job:' + uuid, prop, val, function (err, res) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            }
            return callback();
        });
    }

    backend.updateJobProperty = updateJobProperty;

    // Queue a job which has been running; i.e, due to whatever the reason,
    // re-queue the job. It'll unlock the job, update the status, add the
    // results for every finished task so far ...
    // job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function queueJob(job, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowRedisBackend.queueJob job(Object) required'));
        }
        var multi = client.multi();
        var p;

        for (p in job) {
            if (typeof (job[p]) === 'object') {
                job[p] = JSON.stringify(job[p]);
            }
        }

        if (typeof (job.created_at) === 'string') {
            job.created_at = String(new Date(job.created_at).getTime());
        }
        if (typeof (job.exec_after) === 'string') {
            job.exec_after = String(new Date(job.exec_after).getTime());
        }

        return client.lrem('wf_running_jobs', 0, job.uuid,
            function (err, res) {
                if (err) {
                    log.error({err: err});
                    return callback(new wf.BackendInternalError(err));
                }

                if (res <= 0) {
                    return callback(new wf.BackendPreconditionFailedError(
                      'Only running jobs can be queued again'));
                }
                job.execution = 'queued';
                multi.srem('wf_runner:' + job.runner_id, job.uuid);
                multi.rpush('wf_queued_jobs', job.uuid);
                multi.hmset('job:' + job.uuid, job);
                multi.hdel('job:' + job.uuid, 'runner_id');
                return multi.exec(function (err, replies) {
                    if (err) {
                        log.error({err: err});
                        return callback(new wf.BackendInternalError(err));
                    } else {
                        return getJob(job.uuid, callback);
                    }
                });
            });
    }

    backend.queueJob = queueJob;


    // Get the given number of queued jobs uuids.
    // - start - Integer - Position of the first job to retrieve
    // - stop - Integer - Position of the last job to retrieve, _included_
    // - callback - f(err, jobs)
    // See http://redis.io/commands/lrange for the details about start/stop.
    function nextJobs(start, stop, callback) {
        var slice;

        return client.sort('wf_queued_jobs',
            'by', 'job:*->created_at', 'asc',
            'get', 'job:*->uuid',
            function (err, res) {
                if (err) {
                    log.error({err: err});
                    return callback(new wf.BackendInternalError(err));
                }

                if (res.length === 0) {
                    return callback(null, null);
                }

                slice = res.slice(start, stop + 1);

                if (slice.length === 0) {
                    return callback(null, null);
                } else {
                    return callback(null, slice);
                }
            });
    }

    backend.nextJobs = nextJobs;

    // Register a runner on the backend and report it's active:
    // - runner_id - String, unique identifier for runner.
    // - active_at - ISO String timestamp. Optional. If none is given,
    //   current time
    // - callback - f(err)
    function registerRunner(runner_id, active_at, callback) {
        if (typeof (active_at) === 'function') {
            callback = active_at;
            active_at = new Date().toISOString();
        }
        client.hset(
            'wf_runners',
            runner_id,
            active_at,
            function (err, res) {
                if (err) {
                    log.error({err: err});
                    return callback(new wf.BackendInternalError(err));
                }
                // Actually, we don't care at all about the 0/1 possible return
                // values.
                return callback(null);
            });
    }

    backend.registerRunner = registerRunner;

    // Report a runner remains active:
    // - runner_id - String, unique identifier for runner. Required.
    // - active_at - ISO String timestamp. Optional. If none is given,
    //   current time
    // - callback - f(err)
    function runnerActive(runner_id, active_at, callback) {
        return registerRunner(runner_id, active_at, callback);
    }

    backend.runnerActive = runnerActive;

    // Get the given runner id details
    // - runner_id - String, unique identifier for runner. Required.
    // - callback - f(err, runner)
    function getRunner(runner_id, callback) {
        client.hget('wf_runners', runner_id, function (err, runner) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            }
            return callback(null, new Date(runner));
        });
    }

    backend.getRunner = getRunner;


    // Get all the registered runners:
    // - callback - f(err, runners)
    function getRunners(callback) {
        return client.hgetall('wf_runners', function (err, runners) {
            var theRunners = {};
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            }
            Object.keys(runners).forEach(function (uuid) {
                theRunners[uuid] = new Date(runners[uuid]);
            });
            return callback(null, theRunners);
        });
    }

    backend.getRunners = getRunners;

    // Set a runner as idle:
    // - runner_id - String, unique identifier for runner
    // - callback - f(err)
    function idleRunner(runner_id, callback) {
        client.sadd('wf_idle_runners', runner_id, function (err) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            }
            return callback(null);
        });
    }

    backend.idleRunner = idleRunner;

    // Check if the given runner is idle
    // - runner_id - String, unique identifier for runner
    // - callback - f(boolean)
    function isRunnerIdle(runner_id, callback) {
        client.sismember('wf_idle_runners', runner_id, function (err, idle) {
            if (err || idle === 1) {
                return callback(true);
            } else {
                return callback(false);
            }
        });
    }

    backend.isRunnerIdle = isRunnerIdle;

    // Remove idleness of the given runner
    // - runner_id - String, unique identifier for runner
    // - callback - f(err)
    function wakeUpRunner(runner_id, callback) {
        client.srem('wf_idle_runners', runner_id, function (err) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            }
            return callback(null);
        });
    }

    backend.wakeUpRunner = wakeUpRunner;

    // Get all jobs associated with the given runner_id
    // - runner_id - String, unique identifier for runner
    // - callback - f(err, jobs). `jobs` is an array of job's UUIDs.
    //   Note `jobs` will be an array, even when empty.
    function getRunnerJobs(runner_id, callback) {
        client.smembers('wf_runner:' + runner_id, function (err, jobs) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            }
            return callback(null, jobs);
        });
    }

    backend.getRunnerJobs = getRunnerJobs;


    // Get all the workflows:
    // - callback - f(err, workflows)
    function getWorkflows(callback) {
        var multi = client.multi();

        return client.smembers('wf_workflows', function (err, res) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            }
            res.forEach(function (uuid) {
                multi.hgetall('workflow:' + uuid);
            });
            return multi.exec(function (err, replies) {
                if (err) {
                    log.error({err: err});
                    return callback(new wf.BackendInternalError(err));
                }
                replies.forEach(function (workflow, i, arr) {
                    if (workflow.chain) {
                        workflow.chain = JSON.parse(workflow.chain);
                    }
                    if (workflow.onerror) {
                        workflow.onerror = JSON.parse(workflow.onerror);
                    }
                    replies[i] = workflow;
                });
                return callback(null, replies);
            });
        });
    }

    backend.getWorkflows = getWorkflows;


    // Get all the jobs:
    // - params - JSON Object. Can include the value of the job's "execution"
    //   status, and any other key/value pair to search for into job's params.
    //   - execution - String, the execution status for the jobs to return.
    //                 Return all jobs if no execution status is given.
    //   - limit - Integer, max number of Jobs to retrieve. By default 1000.
    //   - offset - Integer, start retrieving Jobs from the given one. Note
    //              jobs are sorted by "created_at" DESCending.
    // - callback - f(err, jobs)
    function getJobs(params, callback) {
        var multi = client.multi();
        var executions = ['queued', 'failed', 'succeeded',
            'canceled', 'running', 'retried'];
        var list_name;
        var execution;
        var offset;
        var limit;

        if (typeof (params) === 'object') {
            execution = params.execution;
            delete params.execution;
            offset = params.offset;
            delete params.offset;
            limit = params.limit;
            delete params.limit;
        }

        if (typeof (params) === 'function') {
            callback = params;
        }

        if (typeof (limit) === 'undefined') {
            limit = 1000;
        }

        if (typeof (offset) === 'undefined') {
            offset = 0;
        }

        if (typeof (execution) === 'undefined') {
            return client.smembers('wf_jobs', function (err, res) {
                res.forEach(function (uuid) {
                    multi.hgetall('job:' + uuid);
                });

                multi.exec(function (err, replies) {
                    if (err) {
                        log.error({err: err});
                        return callback(new wf.BackendInternalError(err));
                    }
                    replies.forEach(function (job, i, arr) {
                        return _decodeJob(job, function (job) {
                            replies[i] = job;
                        });
                    });


                    if (typeof (params) === 'object' &&
                        Object.keys(params).length > 0) {
                        var theJobs = [];
                        replies.forEach(function (job) {
                            var matches = true;
                            Object.keys(params).forEach(function (k) {
                                if (!job.params[k] ||
                                    job.params[k] !== params[k]) {
                                    matches = false;
                                }
                            });
                            if (matches === true) {
                                theJobs.push(job);
                            }
                        });
                        return callback(null, theJobs.slice(offset, limit));
                    } else {
                        return callback(null, replies.slice(offset, limit));
                    }
                });
            });
        } else if (executions.indexOf(execution !== -1)) {
            list_name = 'wf_' + execution + '_jobs';
            return client.llen(list_name, function (err, res) {
                if (err) {
                    log.error({err: err});
                    return callback(new wf.BackendInternalError(err));
                }
                return client.lrange(
                  list_name,
                  0,
                  (res + 1),
                  function (err, results) {
                    if (err) {
                        return callback(new wf.BackendInternalError(err));
                    }
                    results.forEach(function (uuid) {
                        multi.hgetall('job:' + uuid);
                    });
                    return multi.exec(function (err, replies) {
                        if (err) {
                            log.error({err: err});
                            return callback(new wf.BackendInternalError(err));
                        }
                        replies.forEach(function (job, i, arr) {
                            return _decodeJob(job, function (job) {
                                replies[i] = job;
                            });
                        });

                        if (typeof (params) === 'object' &&
                            Object.keys(params).length > 0) {
                            var theJobs = [];
                            replies.forEach(function (job) {
                                var matches = true;
                                Object.keys(params).forEach(function (k) {
                                    if (!job.params[k] ||
                                        job.params[k] !== params[k]) {
                                        matches = false;
                                    }
                                });
                                if (matches === true) {
                                    theJobs.push(job);
                                }
                            });
                            return callback(null, theJobs.slice(offset, limit));
                        } else {
                            return callback(null, replies.slice(offset, limit));
                        }
                    });
                  });
            });
        } else {
            return callback(new wf.BackendInvalidArgumentError(
              'excution is required and must be one of' +
              '"queued", "failed", "succeeded", "canceled", "running"' +
              ', "retried"'));
        }
    }

    backend.getJobs = getJobs;


    // Add progress information to an existing job:
    // - uuid - String, the Job's UUID.
    // - info - Object, {'key' => 'Value'}
    // - meta - Any additional information to pass to the backend which is not
    //        job info
    // - callback - f(err)
    function addInfo(uuid, info, meta, cb) {

        if (typeof (meta) === 'function') {
            cb = meta;
            meta = {};
        }

        if (typeof (info) === 'object') {
            info = JSON.stringify(info);
        }

        return client.exists('job:' + uuid, function (err, result) {
            if (err) {
                log.error({err: err});
                return cb(new wf.BackendInternalError(err));
            }

            if (result === 0) {
                return cb(new wf.BackendResourceNotFoundError(
                  'Job does not exist. Cannot Update.'));
            }

            return client.rpush('jobinfo:' + uuid, info, function (err, res) {
                if (err) {
                    log.error({err: err});
                    return cb(new wf.BackendInternalError(err));
                }
                return cb();
            });
        });
    }

    backend.addInfo = addInfo;


    // Get progress information from an existing job:
    // - uuid - String, the Job's UUID.
    // - meta - Any additional information to pass to the backend which is not
    //        job info
    // - callback - f(err, info)
    function getInfo(uuid, meta, callback) {
        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        var llen;
        var info = [];

        return client.exists('job:' + uuid, function (err, result) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            }

            if (result === 0) {
                return callback(new wf.BackendResourceNotFoundError(
                  'Job does not exist. Cannot get info.'));
            }

            return client.llen('jobinfo:' + uuid, function (err, res) {
                if (err) {
                    log.error({err: err});
                    return callback(new wf.BackendInternalError(err));
                }
                llen = res;
                return client.lrange(
                  'jobinfo:' + uuid,
                  0,
                  llen,
                  function (err, items) {
                    if (err) {
                        log.error({err: err});
                        return callback(new wf.BackendInternalError(err));
                    }
                    if (items.length) {
                        items.forEach(function (item) {
                            info.push(JSON.parse(item));
                        });
                    }
                    return callback(null, info);
                  });
            });

        });

    }

    backend.getInfo = getInfo;

    return backend;
};
