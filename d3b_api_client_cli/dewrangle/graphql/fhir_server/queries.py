"""
Dewrangle GraphQL query definitions
"""

from gql import gql

organization_fhir_servers = gql(
    """
    query organizationQuery($id: ID!) {
      node(id: $id) {
        id
        ... on Organization {
          id
          name
          fhirServers {
            edges {
              node {
                id
                name
                url
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
    """
)
