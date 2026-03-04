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
          fhirServerDeployments {
            id
            fhirServer {
              id
              name
              url
              authType
              authConfig {
                ...on FhirServerAuthConfigOIDCClientCredential {
                  issuerBaseUrl
                  clientId
                }
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
                fhirServerDeployments {
                  id
                  fhirServer {
                    id
                    name
                    url
                    authType
                    authConfig {
                      ...on FhirServerAuthConfigOIDCClientCredential {
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
  """
)
