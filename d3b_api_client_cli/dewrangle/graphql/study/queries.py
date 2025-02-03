"""
Dewrangle GraphQL query definitions
"""

from gql import gql

study = gql(
    """
    query studyQuery($id: ID!) {
      node(id: $id) {
        id
        ... on Study {
          globalId
          name
          id
          organization {
            name
            id
          }
        }
      }
    }
    """
)

study_by_global_id = gql(
    """
    query studyQuery($id: ID!, $filter: StudyFilter!) {
      node(id: $id) {
        id
        ... on Organization {
          name
          studies(filter: $filter) {
            edges {
              node {
                id
                name
                globalId
              }
            }
          }
        }
      }
    }
    """
)
org_studies = gql(
    """
    query orgStudies($id: ID!, $first: Int, $after: ID) {
      node(id: $id) {
        id
        ... on Organization {
          name
          id
          studies(first: $first, after: $after) {
            totalCount
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              cursor
              node
              {
                id
                globalId
                name
                studyFhirServers {
                  edges {
                    node {
                      id
                      ... on StudyFhirServer {
                        fhirServer {
                          id
                          name
                          url
                          type
                          authType
                          authConfig {
                            ... on FhirServerAuthConfigOIDCClientCredential {
                              issuerBaseUrl
                              clientId
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  """
)
