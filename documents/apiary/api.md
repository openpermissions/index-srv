FORMAT: 1A
HOST: https://index-stage.copyrighthub.org

# Open Permissions Platform Index Service
The Index Service is a routing service for the Open Permissions Platform.

## Standard error output
On endpoint failure there is a standard way to report errors.
The output should be of the form

| Property | Description               | Type   |
| :------- | :----------               | :---   |
| status   | The status of the request | number |
| errors   | A list of errors          | array  |

### Error
| Property         | Description                                 | Type   | Mandatory |
| :-------         | :----------                                 | :---   | :-------  |
| source           | The name of the service producing the error | string | yes       |
| source_id_type   | The type of the source identity             | string | no        |
| source_id        | The source id                               | string | no        |
| message          | A description of the error                  | string | yes       |

# Authorization

This API requires authentication. Where [TOKEN] is indicated in an endpoint header you should supply an OAuth 2.0 access token with the appropriate scope (read, write or delegate). 

See [How to Auth](https://github.com/openpermissions/auth-srv/blob/master/documents/markdown/how-to-auth.md) 
for details of how to authenticate Hub services.

# Group Index

## Index service information [/v1/index]

### Retrieve service information [GET]

| OAuth Token Scope |
| :----------       |
| read              |

#### Output
| Property | Description               | Type   |
| :------- | :----------               | :---   |
| status   | The status of the request | number |
| data     | The service information   | object |

##### Service information
| Property     | Description                    | Type   |
| :-------     | :----------                    | :---   |
| service_name | The name of the api service    | string |
| service_id   | The id of the api service      | string |
| version      | The version of the api service | string |


+ Request
    + Headers

            Accept: application/json

+ Response 200 (application/json; charset=UTF-8)
    + Body

            {
                "status": 200,
                "data": {
                    "service_name": "Open Permissions Platform Index Service",
                    "service_id": "d8936d2ae20f11e597309a79f06e9478",
                    "version": "0.1.0"
                }
            }

# Group repositories

## Repositories by entity [/v1/index/entity-types/{entity_type}/id-types/{source_id_type}/ids/{source_id}/repositories{?related_depth}]

+ Parameters
    + entity_type (required, enum[string])
        Type of entity to add
        + Members
            + `asset` - An asset
    + source_id_type (required, string)
        The type of the entity id
    + source_id (required, string)
        The id
    + related_depth (optional, int)
        An integer representing the maximum distance of related ids.
        This value may be limited by the index service.
        By default this value 0 and relations is empty.


### Get the repositories of an entity [GET]

| OAuth Token Scope |
| :----------       |
| read              |

#### Output
| Property | Description               | Type   |
| :------- | :----------               | :---   |
| status   | The status of the request | number |
| data     | A list of repositories    | array  |

##### Repository
| Property      | Description       | Type   |
| :-------      | :----------       | :---   |
| repository_id | The repository id | string |
| entity_id     | The entity id     | string |


+ Request Entity repositories without relations (application/json)

    + Headers

            Accept: application/json
            Authorization: Bearer [TOKEN]

+ Response 200 (application/json; charset=UTF-8)

    + Body

            {
                "status": 200,
                "data": {
                    "source_id_type": "ISBN",
                    "source_id": "10101-1000",
                    "repositories": [
                        {
                            "repository_id": "hub1",
                            "entity_id": "5d84d36d6eec446aae9c4435291eca8a"
                        },
                        {
                            "repository_id": "hub2",
                            "entity_id": "749ac740da53480d81f8568240e93fb2"
                        }
                    ],
                    "relations": []
                }
            }

+ Request Entity repositories with relations (application/json)

    + Headers

            Accept: application/json
            Authorization: Bearer [TOKEN]

+ Response 200 (application/json; charset=UTF-8)

    + Body

            {
                "status": 200,
                "data": {
                    "source_id_type": "ISBN",
                    "source_id": "10101-1000",
                    "repositories": [
                        {
                            "repository_id": "hub1",
                            "entity_id": "5d84d36d6eec446aae9c4435291eca8a"
                        },
                        {
                            "repository_id": "hub2",
                            "entity_id": "749ac740da53480d81f8568240e93fb2"
                        }
                    ],
                    "relations": [
                        {
                            "to": {
                                "entity_id": "5d84d36d6eec446aae9c4435291e2222",
                                "repository_id": "23e3432e3342342123131"
                            },
                            "via": {
                                "source_id_type": "xid",
                                "source_id": "1000",
                                "entity_id": "749ac740da53480d81f8568240e92222"
                            }
                        }
                    ]
                }
            }

+ Request Entity is not present in the index (application/json)

    + Headers

            Accept: application/json
            Authorization: Bearer [TOKEN]

+ Response 404 (application/json; charset=UTF-8)

    + Body

            {
                "status": 404,
                "errors": [
                    {
                        "source": "index",
                        "message": "Not found"
                    }
                ]
            }

## Repositories by Entities [/v1/index/entity-types/{entity_type}/repositories{?related_depth}]

+ Parameters
    + entity_type (required, enum[string])
        Type of entity to add
        + Members
            + `asset` - An asset
    + related_depth (optional,  int)
        An integer specifying how deep the index service must search for related ids.
        This value may be limited by the index service.
        By default this value 0 and relations is empty.
        
### Get the repositories for multiple entities [POST]

| OAuth Token Scope |
| :----------       |
| read              |

#### Input
List of source_id_type and source_id value pairs.

| Property       | Description                    | Type   | Mandatory |
| :-------       | :----------                    | :---   | :----     |
| source_id_type | The type of the asset identity | string | yes       |
| source_id      | The asset identity             | string | yes       |

#### Output
| Property | Description                                                                        | Type   |
| :------- | :----------                                                                        | :---   |
| status   | The status of the request                                                          | number |
| data     | The input array with an array of repositories added as an attribute to each object | array  |

##### Repository
| Property      | Description       | Type   |
| :-------      | :----------       | :---   |
| repository_id | The repository id | string |
| entity_id     | The entity id     | string |



+ Request Entity repositories (application/json)

    + Headers

            Accept: application/json
            Authorization: Bearer [TOKEN]

    + Body

            [
                {
                    "source_id": "12489191279b443ab4e1a899f7ed84c8",
                    "source_id_type": "hub_key"
                },
                {
                    "source_id": "a_foo_id",
                    "source_id_type": "foo_id_type"
                }
            ]

+ Response 200 (application/json; charset=UTF-8)

    + Body

            {
                "status": 200,
                "data": [
                    {
                        "source_id": "12489191279b443ab4e1a899f7ed84c8",
                        "source_id_type": "hub_key",
                        "repositories": [
                            {
                                "repository_id": "0fa220b384014735e04632463c1c3092",
                                "entity_id": "12489191279b443ab4e1a899f7ed84c8"
                            }
                        ],
                        "relations": []
                    },
                    {
                        "source_id": "a_foo_id",
                        "source_id_type": "foo_id_type",
                        "repositories": [
                            {
                                "repository_id": "0fa220b384014735e04632463c1c3092",
                                "entity_id": "a_foo_id"
                            }
                        ],
                        "relations": []
                    }
                ]
            }

+ Request Entity is not present in the index (application/json)

    + Headers

            Accept: application/json
            Authorization: Bearer [TOKEN]

    + Body

            [
                {
                    "source_id": "a_foo_id",
                    "source_id_type": "foo_id_type"
                }
            ]

+ Response 200 (application/json; charset=UTF-8)

    + Body

            {
                "status": 200,
                "errors": [
                    {
                        "source_id": "a_foo_id",
                        "source_id_type": "foo_id_type",
                        "repositories": [],
                        "relations": []
                    }
                ]
            }

# Group Indexed
Endpoint to retrieve timestamp of when repository was last indexed

## Indexed [/v1/index/repositories/{repository_id}/indexed]

+ Parameters
    + repository_id (required, enum[string])
        The id of the repository

### Get last indexed [GET]

| OAuth Token Scope |
| :----------       |
| read              |


#### Output
| Property     | Description                | Type   |
| :-------     | :----------                | :---   |
| status       | The status of the request  | number |
| last_indexed | ISO-formatted timestamp    | string |


+ Request
    + Headers

            Accept: application/json
            Authorization: Bearer [TOKEN]

+ Response 200 (application/json; charset=UTF-8)
    + Body

            {
                "status": 200,
                "last_indexed": "2016-07-08T09:10:47+00:00"
            }
            
            
# Group Notifications
Endpoint to send notifications of new data in a repository

## Notifications [/v1/index/notifications]

### Notify service that repo has new data [POST]

| OAuth Token Scope |
| :----------       |
| write             |

##### Input

| Property | Description          | Type   |
| :------- | :----------          | :---   |
| id       | The repository's ID  | string |

#### Output
| Property | Description                                                                        | Type   |
| :------- | :----------                                                                        | :---   |
| status   | The status of the request                                                          | number |


+ Request
    + Headers

            Accept: application/json
            Authorization: Bearer [TOKEN]

    + Body

            {
                "id": "0fa220b384014735e04632463c1c3092"
            }

+ Response 200 (application/json; charset=UTF-8)
    + Body

            {
                "status": 200
            }
