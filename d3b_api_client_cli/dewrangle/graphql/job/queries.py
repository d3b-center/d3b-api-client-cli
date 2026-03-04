"""
Dewrangle GraphQL query definitions
"""

from gql import gql

fhir_resource_ingest_job = gql(
    """
    query fhirResourceIngestJobQuery($id: ID!) {
      node(id: $id) {
        id
        ... on Job {
          operation
          completedAt
          result {
            ... on JobResultFhirResource {
              resources {
                count
                resourceType
              }
            }
          }
          errors {
            edges {
              node {
                id
                name
                message
              }
            }
          }
    
        }
      }
    }
    """
)

job_status_query = gql(
    """
    query jobStatusQuery($id: ID!) {
      node(id: $id) {
        id
        ... on Job {
          operation
          completedAt
          errors {
            edges {
              node {
                id
                name
                message
              }
            }
          }
    
        }
      }
    }
    """
)
