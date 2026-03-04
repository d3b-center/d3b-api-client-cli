"""
Dewrangle GraphQL query definitions
"""

from gql import gql

viewer_query = gql(
    """
    query($first: Int, $after: ID) {
      viewer {
        name
        organizationUsers {
          edges {
            node {
              organization {
                id
                name
                description
                email
                website
                fhirServers {
                  edges {
                    node {
                      id
                      name
                    }
                  }
                }
                studies(first: $first, after: $after) {
                  edges {
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
        }
      }
}
    """
)
