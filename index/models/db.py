# -*- coding: utf-8 -*-
# Copyright © 2014-2016 Digital Catapult and The Copyright Hub Foundation
# (together the Open Permissions Platform Coalition)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

from __future__ import unicode_literals
import re
import csv
import urllib
import logging
import json
import string

from bass import hubkey
from koi import exceptions
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado import gen, options

HUB_KEY = "hub_key"

DEFAULT_PAGER_LIMIT = getattr(options, "default_pager_limit", 1024)

NS = {
    "chubindex": "http://digicat.io/ns/chubindex/1.0/",
    "op": "http://digicat.io/ns/op/1.0/",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "id": "http://openpermissions.org/ns/id/"
}

SPARQL_PREFIXES = "\n".join(map(lambda i: "PREFIX %s: <%s>" % i, NS.items()))
TURTLE_PREFIXES = "\n".join(map(lambda i: "@prefix %s: <%s> ." % i, NS.items()))

NAMESPACE_ASSET = """
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
  <entry key="com.bigdata.rdf.sail.namespace">{}</entry>
</properties>
"""

# %s is the initial query that build entity_uri
# the starting point of the query
LEVEL_1_REL_SUBQUERY = """
{
    SELECT ?via_hk ?via_id ?to_hk WHERE {
        %s .
        BIND (?entity_uri AS ?via_hk) .
        ?via_hk op:alsoIdentifiedBy ?via_id.
        ?via_id ^op:alsoIdentifiedBy? ?to_hk .
        FILTER (?to_hk != ?via_hk) .
    }
}
"""

LEVEL_N_REL_SUBQUERY = string.Template("""
$from_hk op:alsoIdentifiedBy $via_id .
FILTER ( $via_id != ?origid ) .
$via_id  ^op:alsoIdentifiedBy $to_hk .
FILTER ( $to_hk NOT IN ( $forbidden ) ) .
""")

OUTER_REL_SUBQUERY = string.Template("""
{ SELECT ?group (CONCAT("[", GROUP_CONCAT(?json;separator=","),"]") AS ?relations ) WHERE {
    BIND ( "constant" as ?group ) .
    { SELECT DISTINCT ?to_hk ?to_repo ?via_id ?via_id_id_value ?via_id_id_type ?via_hk WHERE {
       $relquery

       OPTIONAL { ?via_id chubindex:id ?via_id_id_value . }
       OPTIONAL { ?via_id chubindex:id_type ?via_id_id_type . }
       OPTIONAL { ?to_hk chubindex:repo ?to_repo . }
    }
}
BIND (CONCAT("{\\"to\\": {\\"entity_id\\": \\"", STRAFTER(STR(?to_hk),STR(id:)) , "\\", \\"repository_id\\": \\"", ?to_repo,
             "\\" }, \\"via\\": {\\"source_id\\" : \\"", ?via_id_id_value, "\\", \\"source_id_type\\": \\"", ?via_id_id_type,
             "\\", \\"entity_id\\" : \\"", STRAFTER(STR(?via_hk),STR(id:)), "\\" } }" ) AS ?json)
}
GROUP BY ?group
}
""")

# This is the main query for querying an entry (a single part source_id, source_id_type) in the index
#
# Fist part of the query retrieve repositories directly associated with the id.
# Second part of the query is $relquery and is built in python to a specified recursion level,
# Thirs part just reassociates the query parameters to the results.
QUERY_TEMPLATE = string.Template("""
{
    {
        SELECT ?group (CONCAT("[", GROUP_CONCAT(?json; separator=","),"]") AS ?repositories ) {
           BIND ( "constant" as ?group ) .
           $initial_query .
           ?entity_uri chubindex:repo ?repo_id .
           BIND (CONCAT("{\\"repository_id\\":\\"",?repo_id,"\\",\\"entity_id\\":\\"",STRAFTER(STR(?entity_uri),STR(id:)),"\\"}") AS ?json).
        } GROUP BY ?group
    }

    $relquery

    BIND ( $source_id_bind AS ?source_id ) .
    BIND ( "$source_id_type" AS ?source_id_type ) .
}
""")

FIND_ENTITY_TEMPLATE = string.Template("""
SELECT DISTINCT ?s
WHERE {?s ?p ?o;
<http://digicat.io/ns/chubindex/1.0/repo> "$repository_id"^^xsd:string.
	?o  	<http://digicat.io/ns/chubindex/1.0/id>  ?id;
<http://digicat.io/ns/chubindex/1.0/id_type>  ?idtype
		.
		VALUES (?id ?idtype) {
		$id_filter
		}
	}
""")

SOURCE_ID_FILTER_TEMPLATE = string.Template("""
("$id"^^xsd:string "$id_type"^^xsd:string)
""")

FIND_ENTITY_SOURCE_IDS_TEMPLATE = string.Template("""
SELECT DISTINCT ?id ?idtype
WHERE {<$entity_id> ?p ?o.
?o  	<http://digicat.io/ns/chubindex/1.0/id>  ?id;
<http://digicat.io/ns/chubindex/1.0/id_type> ?idtype.}
""")

FIND_ENTITY_COUNT_BY_SOURCE_IDS_TEMPLATE = string.Template("""
SELECT (COUNT(?s) AS ?count)
WHERE {?s ?p ?o.
		?o 
           <http://digicat.io/ns/chubindex/1.0/id> "$source_id"^^xsd:string;
			<http://digicat.io/ns/chubindex/1.0/id_type> "$source_id_type"^^xsd:string
			.    
      FILTER NOT EXISTS {<$entity_id> ?p ?o}
                 }
""")

DELETE_ID_TRIPLES_TEMPLATE = string.Template("""
DELETE
WHERE {?s 
           <http://digicat.io/ns/chubindex/1.0/id> "$source_id"^^xsd:string;
			<http://digicat.io/ns/chubindex/1.0/id_type> "$source_id_type"^^xsd:string;
      ?p ?o.}
""")

DELETE_ENTITY_TRIPLE_TEMPLATE = string.Template("""
DELETE
WHERE {
<$entity_id> ?p ?o}
""")

class DbInterface(object):
    def __init__(self, base_path, port, path, schema):
        """
        :param base_path: ie. "http://localhost"
        :param port: ie. "8080"
        :param path: ie. "/bigdata/namespace/"
        :param schema: ie. "kb"
        """
        self.db_url = "{0}:{1}{2}{3}".format(base_path, port, path, schema)
        self.db_namespace = self.db_url.split('/')[-1]
        self.db_namespace_url = '/'.join(self.db_url.split('/')[:-1])

    @gen.coroutine
    def create_namespace(self):
        """
        Create the namespace in the database
        """
        data = NAMESPACE_ASSET.format(self.db_namespace).strip()
        client = AsyncHTTPClient()
        headers = {'Content-Type': 'application/xml'}

        try:
            yield client.fetch(self.db_namespace_url, method='POST',
                               body=data, headers=headers)
        except HTTPError as e:
            # Namespace already exists with a 409 error
            if e.code != 409:
                logging.error("Database server error({0}) (Is Database server"
                              " running on {1} HTTP Error {2})".format(
                    e.message, self.db_url, e.code))
        except Exception as e:
            msg = ("Database server error({0}) (Is Database server"
                   " running on {1})".format(e.args, self.db_url))
            logging.error(msg)

    def map_to_entity_type(self, entity_type):
        """
        Reserved to Eventually, map to ontology - or use specific model.
        Current implementation keeps entity_type as string.
        """
        return urllib.quote_plus(entity_type)

    @gen.coroutine
    def _run_query(self, query):
        if type(query) == unicode:
            query = query.encode('ascii')

        headers = {'Accept': 'text/csv'}

        rsp = yield AsyncHTTPClient().fetch(
            self.db_url, method="POST",
            body=urllib.urlencode({'query': SPARQL_PREFIXES + query}),
            headers=headers)

        raise gen.Return(list(csv.DictReader(rsp.buffer)))

    @gen.coroutine
    def _run_update(self, query):
        if type(query) == unicode:
            query = query.encode('ascii')

        headers = {'Accept': 'text/csv'}

        rsp = yield AsyncHTTPClient().fetch(
            self.db_url, method="POST",
            body=urllib.urlencode({'update': SPARQL_PREFIXES + query}),
            headers=headers)

        raise gen.Return(list(csv.DictReader(rsp.buffer)))

    def _format_relation_subquery(self, source_id_type, source_id, initial_query, maxdepth=2):
        if not maxdepth:
            # we didn't asked for relations... so let just return nothing
            return "BIND (\"[]\" AS ?relations) ."

        # LEVEL 1 query
        mrexpr = [LEVEL_1_REL_SUBQUERY % (initial_query,)]

        # DEEPER QUERIES (BUILD RECURSIVELY)
        for i in range(1, maxdepth):
            cexpr = list()
            cexpr.append(initial_query + ".")
            cexpr.append("BIND (?entity_uri AS ?via_hk0) .")
            for j in range(i):
                forbidden_nodes = " , ".join(map(lambda ci: "?via_hk%d" % (ci,), range(j + 1)))
                cexpr.append(LEVEL_N_REL_SUBQUERY.substitute(
                    via_id="?via_id" + str(j + 1),
                    to_hk="?via_hk" + str(j + 1),
                    from_hk="?via_hk" + str(j),
                    forbidden=forbidden_nodes
                )
                )

            forbidden_nodes = " , ".join(map(lambda ci: "?via_hk%d" % (ci,), range(i + 1)))
            cexpr.append("BIND (?via_hk%d as ?via_hk) ." % (i,))
            cexpr.append(LEVEL_N_REL_SUBQUERY.substitute(
                via_id="?via_id",
                to_hk="?to_hk",
                from_hk="?via_hk",
                forbidden=forbidden_nodes
            )
            )
            cexpr = "{ SELECT ?via_hk ?via_id ?to_hk WHERE { \n %s \n } }\n" % ("\n".join(cexpr),)
            mrexpr.append(cexpr)

        # UNION FOR LEVEL FROM 1 TO N
        return OUTER_REL_SUBQUERY.substitute(relquery="{ %s }" % (" UNION ".join(mrexpr),))

    def _format_subquery(self, source_id_type, source_id, maxdepth=2):
        """
        Get list of repositories and relations for one query (pair source_id_type, source_id)

        :param source_id_type: id type
        :param source_id: id

        :param related_depth: maximum depth when searching for related ids.

        :returns: a list of dictionaries containing "id", "id_type" &
                  "repository"
        """
        # NOTE:
        # Blazegraph doesn't support nice sparql pathexpr with limited length
        #  ?a r{0,10} ?b
        # so it is better to implement this by hand
        # also even with range-length match it is impossible with
        # SPARQL expressions to simply avoid paths and to get shortest path
        # in a graph that is not a tree.

        if source_id_type == HUB_KEY:
            # NOTE: Here we expect entity_id. It is assumed that query to hub_key are resolved via
            # via the resolution service, i.e.  or contacting directly the right repo for hubkey s1,
            # or using the source_id_type and source_id for hubkey s0
            if source_id.startswith("http://openpermissions.org/ns/id/"):
                source_id = source_id.split('/')[-1]
            initial_query = "  BIND ( id:{entity_id} AS ?entity_uri ) ".format(entity_id=str(source_id))
        else:
            initial_query = "  <https://digicat.io/ns/xid/{source_id_type}/{source_id}> ^op:alsoIdentifiedBy ?entity_uri"
            initial_query = initial_query.format(source_id_type=source_id_type, source_id=source_id)

        relquery = self._format_relation_subquery(source_id_type, source_id, initial_query, maxdepth)

        query = QUERY_TEMPLATE.substitute(source_id_bind=("id:%s" if (source_id_type == HUB_KEY) else '"%s"') % (source_id,),
                                          source_id_type=source_id_type,
                                          relquery=relquery,
                                          initial_query=initial_query)
        return query

    @gen.coroutine
    def _getMatchingEntities(self, ids, repository_id):
        """
        Get list of entities matching the given incoming ids 

        :param ids: a list of dictionaries containing "source_id" & "source_id_type".
        :param repository_id: the repo to search.

        :returns: a list of entity ids
        """
        subqueries = [SOURCE_ID_FILTER_TEMPLATE.substitute(id = x['source_id'], id_type = x['source_id_type'])
                      for x in ids]

        query = FIND_ENTITY_TEMPLATE.substitute(repository_id = repository_id, id_filter = ''.join(subqueries))

        logging.debug(query)
        queryresults = yield self._run_query(query)
        logging.debug(queryresults)

        results = [x['s'] for x in queryresults]
        raise gen.Return(results)

    @gen.coroutine
    def _getEntityIdsAndTypes(self, entity_id):
        """
        Get list of source_ids and source_id_types for a given entity id

        :param entity_id: the entity id

        :returns: a list of source_id and source_id_types
        """
        query = FIND_ENTITY_SOURCE_IDS_TEMPLATE.substitute(entity_id = entity_id)

        logging.debug(query)
        queryresults = yield self._run_query(query)
        logging.debug(queryresults)

        results = [{'source_id_type': x['idtype'], 'source_id': x['id']} for x in queryresults]

        raise gen.Return(results)

    @gen.coroutine
    def _countMatchesNotIncluding(self, idAndType, entity_id):
        """
        Count the number of entities using these ids/types (other than this entity)

        :param: idAndType: the source_id and source_id_type to search for
        :param entity_id: the entity id of the entity to exclude

        :returns: a count of entities
        """
        logging.debug('idandtype ' + str(idAndType))
        query = FIND_ENTITY_COUNT_BY_SOURCE_IDS_TEMPLATE.substitute(source_id_type = idAndType['source_id_type'],
                                                                    source_id = idAndType['source_id'],
                                                                    entity_id = entity_id)

        logging.debug(query)
        queryresults = yield self._run_query(query)
        logging.debug(queryresults)

        raise gen.Return(queryresults)

    @gen.coroutine
    def _deleteIds(self, idAndType):
        """
        Delete id/type triples

        :param: idAndType: the source_id and source_id_type to delete

        :returns: nothing
        """
        logging.debug('delete idandtype ' + str(idAndType))
        query = DELETE_ID_TRIPLES_TEMPLATE.substitute(source_id_type = idAndType['source_id_type'],
                                                                    source_id = idAndType['source_id'])

        logging.debug(query)
        queryresults = yield self._run_update(query)
        logging.debug(queryresults)

        raise gen.Return()    

    @gen.coroutine
    def _deleteEntity(self, entity_id):
        """
        Delete entity triples

        :param: entity_id: the id of the entity to delete

        :returns: nothing
        """
        logging.debug('delete entity_id ' + str(entity_id))
        query = DELETE_ENTITY_TRIPLE_TEMPLATE.substitute(entity_id = entity_id)

        logging.debug(query)
        queryresults = yield self._run_update(query)
        logging.debug(queryresults)

        raise gen.Return()    

    @gen.coroutine
    def _query_ids(self, ids, related_depth=0):
        """
        Get list of repositories

        :param ids: a list of dictionaries containing "id" & "id_type".
        :param related_depth: maximum depth when searching for related ids.

        :returns: a list of dictionaries containing "id", "id_type" &
                  "repository"
        """
        subqueries = [self._format_subquery(x['source_id_type'], x['source_id'], related_depth)
                      for x in ids]

        query = """
            SELECT DISTINCT ?source_id ?source_id_type ?repositories ?relations
            WHERE {{ {subquery} }}
            ORDER BY ?source_id ?source_id_type
        """.format(subquery=' UNION '.join(subqueries))

        logging.debug(query)
        queryresults = yield self._run_query(query)

        results = []
        in_results = {}

        # reformat the results that are in the index
        for x in queryresults:
            nx = x.copy()
            if nx['source_id_type'] == HUB_KEY:
                nx['source_id'] = str(nx['source_id']).split('/')[-1]
            nx['repositories'] = json.loads(x['repositories'])
            nx['relations'] = []
            for r in json.loads(x['relations']):
                nr = r.copy()
                nr['via']['source_id_type'] = urllib.unquote_plus(r['via']['source_id_type'])
                nr['via']['source_id'] = urllib.unquote_plus(r['via']['source_id'])
                nx['relations'].append(nr)
            results.append(nx)
            in_results[(nx['source_id_type'], nx['source_id'])] = 1

        # add entries for the elements that have not been found in the index
        for x in ids:
            if (x['source_id_type'], x['source_id']) not in in_results:
                nx = x.copy()
                nx['source_id_type'] = urllib.unquote_plus(x['source_id_type'])
                nx['source_id'] = urllib.unquote_plus(x['source_id'])
                if nx['source_id_type'] == HUB_KEY:
                    nx['source_id'] = str(nx['source_id']).split('/')[-1]
                nx['repositories'] = []
                nx['relations'] = []
                results.append(nx)

        raise gen.Return(results)

    @gen.coroutine
    def query(self, ids, related_depth=0):
        """
        Get repositories for a set of entities

        :param ids: a list of dictionaries containing "id" & "id_type"
        :param related_depth: maximum depth when searching for related ids.
        """
        validated_ids, errors = [], []

        for x in ids:                        # source_id / source_id_type
            if 'id' in x:
                x['source_id'] = x['id']
                x['source_id_type'] = x['id_type']
            if 'source_id_type' not in x or 'source_id' not in x:
                errors.append(x)
            elif x['source_id_type'] == HUB_KEY:
                try:
                    parsed = hubkey.parse_hub_key(x['source_id'])
                    x['source_id'] = parsed['entity_id']
                    validated_ids.append(x)
                except ValueError:
                    errors.append(x)
            else:
                # NOTE: internal representation of the index will use
                # id_type and id to construct URI and assusmes that id_type
                # and and have been url_quoted
                x['source_id'] = urllib.quote_plus(x['source_id'])
                x['source_id_type'] = urllib.quote_plus(x['source_id_type'])
                validated_ids.append(x)

        if errors:
            raise exceptions.HTTPError(400, errors)

        result = yield self._query_ids(validated_ids, related_depth)

        raise gen.Return(result)

    @gen.coroutine
    def delete(self, entity_type, ids, repository_id):
        """
        Delete the triples relating to an entity (if they're not used
        by another entity)

        :param entity_type: the type of the entity
        :param ids: a list of dictionaries containing "id" & "id_type"
        :param repository_id: the repository from which to delete
        """
        
        entity_type = self.map_to_entity_type(entity_type)

        validated_ids, errors = [], []

        for x in ids:                        # source_id / source_id_type
            if 'id' in x:
                x['source_id'] = x['id']
                x['source_id_type'] = x['id_type']
            if 'source_id_type' not in x or 'source_id' not in x:
                errors.append(x)
            elif x['source_id_type'] == HUB_KEY:
                try:
                    parsed = hubkey.parse_hub_key(x['source_id'])
                    x['source_id'] = parsed['entity_id']
                    validated_ids.append(x)
                except ValueError:
                    errors.append(x)
            else:
                # NOTE: internal representation of the index will use
                # id_type and id to construct URI and assusmes that id_type
                # and and have been url_quoted
                x['source_id'] = urllib.quote_plus(x['source_id'])
                x['source_id_type'] = urllib.quote_plus(x['source_id_type'])
                validated_ids.append(x)

        if errors:
            raise exceptions.HTTPError(400, errors)

        # get all the entities that match the the ids in this repo
        entities = yield self._getMatchingEntities(validated_ids, repository_id)

        logging.debug('searching for ids ' + str(validated_ids))
        logging.debug('found entities ' + str(entities))

        # for each entity find all the ids associated with it
        for entity in entities:
            idsAndTypes = yield self._getEntityIdsAndTypes(entity)
            logging.debug('for entity ' + str(entity) + ' got these ids ' + str(idsAndTypes))
            assetMatch = yield self._checkIdsAndTypesIdentical(validated_ids, idsAndTypes)
            logging.debug('identical? : ' + str(assetMatch))

            if assetMatch:
                # loop through each set of ids for this entity
                for idAndType in idsAndTypes:
                    # if these ids are NOT used for anything else then delete them
                    result = yield self._countMatchesNotIncluding(idAndType, entity)
                    count = int(result[0].get('count', '0'))

                    if count == 0:
                        yield self._deleteIds(idAndType)
                
                # delete the entity itself
                yield self._deleteEntity(entity)
            
        raise gen.Return()

    @gen.coroutine
    def _checkIdsAndTypesIdentical(self, searchIds, assetIds):
        logging.debug('compariong ' + str(searchIds) + ' : ' + str(assetIds))
        if not [s for s in searchIds if (s['source_id_type'], s['source_id']) not in {(a['source_id_type'].replace('http://openpermissions.org/ns/hub/', ''), a['source_id']) for a in assetIds}] \
            and not [a for a in assetIds if (a['source_id_type'].replace('http://openpermissions.org/ns/hub/', ''), a['source_id']) not in {(s['source_id_type'], s['source_id']) for s in searchIds}]:
            raise gen.Return(True)
        else:
            raise gen.Return(False)

    @gen.coroutine
    def add_entities(self, entity_type, data, repo):
        """
        Transform JSON identifiers and store in the index
        """
        entity_type = self.map_to_entity_type(entity_type)

        turtle = TURTLE_PREFIXES
        template = """
        <https://digicat.io/ns/xid/{source_id_type}/{source_id}>
        chubindex:id "{source_id}"^^xsd:string ;
        chubindex:id_type "{source_id_type}"^^xsd:string .

        <{entity_uri}> op:alsoIdentifiedBy <https://digicat.io/ns/xid/{source_id_type}/{source_id}>;
        chubindex:repo "{repository_id}"^^xsd:string ;
        chubindex:type "{entity_type}"^^xsd:string .
        """
        errors = []
        try:
            for row in data:
                logging.debug(row)
                # validate data to avoid "turtle injection
                # "
                skip = False
                for f in ["entity_id", "source_id", "source_id_type"]:
                    if f not in row:
                        e = 'missing field entity_id skipping record %r'%(row,)
                        errors.append(e)
                        logging.warning("Error processing new record: %s" % (e,))
                        skip = True
                        break
                if skip:
                    continue

                if not re.match("[0-9a-f]{1,64}", row["entity_id"]):
                    e = 'skipping record %s - invalid id ' % (row["entity_id"],)
                    errors.append(e)
                    logging.info("Error processing new record: %s" % (e,))
                    continue

                row["entity_uri"] = NS['id']+row["entity_id"]
                row["source_id_type"] = urllib.quote_plus(row["source_id_type"])
                row["source_id"] = urllib.quote_plus(row["source_id"])

                if not re.match(r"^({})$".format(hubkey.PARTS_S0["id_type"]), row["source_id_type"]):
                    e = 'skipping record %s - invalid id type "%s"' % (row["entity_id"], row["source_id_type"])
                    errors.append(e)
                    logging.warning("Error processing new record: %s" % (e,))
                    continue

                if not re.match(r"^({})$".format(hubkey.PARTS_S0["entity_id"]), row["source_id"]):
                    e = 'skipping record %s - invalid id type "%s"' % (row["entity_uri"], row["source_id"])
                    errors.append(e)
                    logging.warning("Error processing new record: %s" % (e,))
                    continue

                row['repository_id'] = repo
                row['entity_type'] = entity_type
                turtle += template.format(**row)
        except Exception, e:
            data = []
            logging.exception("Error parsing data from repo")

        logging.debug(turtle.strip())
        logging.info('storing %r records' % (len(data),))
        yield self.store(turtle.strip(), 'text/turtle')

        raise gen.Return({"errors": errors, "records": len(data)})

    @gen.coroutine
    def store(self, data, content_type):
        """
        Async function to store data in the database

        :param data: String
        :param content_type: String
        """
        client = AsyncHTTPClient()
        headers = {'Content-Type': content_type}
        try:
            yield client.fetch(self.db_url, method='POST',
                               body=data, headers=headers)
        except HTTPError as e:
            logging.error("Database server error({0}) (Is Database server"
                          " running on {1} HTTP Error {2})".format(e.message, self.db_url, e.code))
        except Exception as e:
            logging.error("Database server error({0}) (Is database server"
                          " running on {1}) {2}".format(e.args, self.db_url, e))
