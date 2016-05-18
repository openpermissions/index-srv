The Open Permissions Plaform Index Service
==========================================

Useful Links
============
* [Open Permissions Platform](http://openpermissions.org)
* [Low level Design](https://github.com/openpermissions/index-srv/blob/master/documents/markdown/low-level-design.md)
* [API Documentation] (https://github.com/openpermissions/index-srv/blob/master/documents/apiary/api.md)

Service Overview
================
This repository contains the Index service which acts as a routing service.
It includes a scheduling service which periodically polls repository services
for newly added assets and it keeps track of that information for quick retrieval 
by storing it in a local database (using shelve).

Running locally
---------------
#### blazegraph data store

Follow the instruction on [blazegraph](https://www.blazegraph.com/) to set up a data store for the index service.

#### index service application
```
pip install -r requirements/dev.txt
python setup.py develop
python index/
```

To show a list of available CLI parameters:

```
python index/ -h [--help]
```

To start the service using test.service.conf:

```
python index/ -t [--test]
```


Locally configurable options
----------------------------

### Service options
| Option name        | Description                           |
|:-------------      |:-------------                         |
| name               | name of service                       |
| service_type       | type of service, in this case "index" |
| service_id         | the ID of the index service           |
| port               | local port the serivce will run on    |
| processes          | number of concurrent processes        |

### Logging options
| Option name        | Description                                |
|:-------------      |:-------------                              |
| log_to_stderr      | outputs logging to console                 |
| log_file_prefix    | file name of log to use                    |
| syslog_host        | IP of external syslog server               |
| syslog_port        | Port that the syslog server listens on     |
| env                | name of environment, "dev" for development |

### Service capabilities
| Option name        | Description                                           |
|:-------------      |:-------------                                         |
| url_registry_db    | URL to registry db                                    |
| url_accounts       | URL of the accounts service that index will use       |
| url_authentication | URL of the authentication service that index will use |
| url_identity       | URL of the identity service that index will use       |
| url_onboarding     | URL of the onboarding service that index will use     |
| url_query          | URL of the query service that index will use          |
| url_repository     | URL of the repository service that index will use     |
| url_transformation | URL of the transformation service that index will use |

### Blazegraph database options
| Option name        | Description                                        |
|:-------------      |:-------------                                      |
| url_index_db       | URL of the running instance of blazegraph          |
| index_db_port      | Port of the running instance of blazegraph         |
| index_db_path      | Path to namespace in blazegraph                    |
| index_schema       | Namespace in blazegraph used by the index service  |
| env                | name of environment, "dev" for development         |

### SSL options
| Option name      | Description                                                |
|:-------------    |:-------------                                              |
| use_ssl          | Turn SSL on / off                                          |
| ssl_key          | Local path to ssl key to use                               |
| ssl_cert         | Local path to ssl certificate to use                       |
| ssl_ca_cert      | Local path to ssl CA certificate                           |
| ssl_cert_reqs    | Levels of use of SSL: 0 (None), 1 (Optional), 2 (Required) |

### Polling and notification options
| Option name                  | Description                                             |
|:-------------                |:-------------                                           |
| poll_repositories            | Turn polling on / off                                   |
| notifications_queue_max_size | Max size of notifications queue                         |
| accounts_poll_interval       | Polling interval in seconds                             |
| default_poll_interval        | Default polling interval in seconds (86400, once a day) |
| notify_min_delay             | Minimum delay between scans in seconds                  |

### Misc
| Option name   | Description                                 |
|:------------- |:-------------                               |
| cors          | Turn Cross-origin resource sharing on / off |


Running tests and generating code coverage
------------------------------------------
To have a "clean" target from build artifacts:

```
make clean
```

To install requirements. By default prod requirement is used:

```
make requirements [REQUIREMENT=test|dev|prod]
```

To run all unit tests and generate a HTML code coverage report along with a
JUnit XML report in tests/unit/reports:

```
make test
```

To run pyLint and generate a HTML report in tests/unit/reports:

```
make pylint
```

To run create the documentation for the service in _build:

```
make docs
```
