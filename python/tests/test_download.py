"""Download Tests"""
import pytest
import databusclient.client as cl

DEFAULT_ENDPOINT="https://databus.dbpedia.org/sparql"
TEST_QUERY="""
PREFIX dcat: <http://www.w3.org/ns/dcat#>
SELECT ?x WHERE {
  ?sub dcat:downloadURL ?x .
} LIMIT 10
"""
TEST_COLLECTION="https://databus.dbpedia.org/dbpedia/collections/dbpedia-snapshot-2022-12"

def test_with_query():
  cl.download("target",DEFAULT_ENDPOINT,[TEST_QUERY]

)
  
def test_with_collection():
  cl.download("target",DEFAULT_ENDPOINT,[TEST_COLLECTION])