"""
Dewrangle GraphQL mutation definitions
"""

from gql import gql

create_fhir_server = gql(
    """
    mutation fhirServerCreateMutation($input: FhirServerCreateInput!) {
    fhirServerCreate(input: $input) {
      errors {
        ... on MutationError {
          __typename
          message
          field
        }
      }
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
  """
)
update_fhir_server = gql(
    """
    mutation fhirServerUpdateMutation(
      $id: ID!
      $input: FhirServerUpdateInput!
    ) {
      fhirServerUpdate(id: $id, input: $input) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
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
  """
)

delete_fhir_server = gql(
    """
    mutation fhirServerDeleteMutation($id: ID!) {
    fhirServerDelete(id: $id) {
      errors {
        ... on MutationError {
          __typename
          message
          field
        }
      }
      fhirServer {
        id
        name
      }
    }
  }
  """
)
