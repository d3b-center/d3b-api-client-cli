"""
Dewrangle GraphQL mutation definitions
"""

from gql import gql


create_credential = gql(
    """
    mutation credentialCreateMutation($input: CredentialCreateInput!) {
      credentialCreate(input: $input) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
        credential {
          id
          name
          key
          study {
            name
            id
            globalId
          }
        }
      }
    }
    """
)
update_credential = gql(
    """
    mutation credentialUpdateMutation($id: ID!, $input: CredentialUpdateInput!) {
      credentialUpdate(id: $id, input: $input) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
        credential {
          id
          name
          key
          study {
            name
            id
            globalId
          }
        }
      }
    }
    """
)

delete_credential = gql(
    """
    mutation credentialDeleteMutation($id: ID!) {
      credentialDelete(id: $id) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
        credential {
          id
          name
          key
          study {
            name
            id
            globalId
          }
        }
      }
    }
    """
)
