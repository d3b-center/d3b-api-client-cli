"""
Dewrangle GraphQL mutation definitions
"""

from gql import gql


create_volume = gql(
    """
    mutation volumeCreateMutation($input: VolumeCreateInput!) {
    volumeCreate(input: $input) {
      errors {
        ... on MutationError {
          __typename
          message
          field
        }
      }
      volume {
        id
        name
        region
        type
        pathPrefix
        study {
          id
          globalId
        }
        credential {
          id
          type
          key
        }
      }
    }
  }
    """
)
update_volume = gql(
    """
    mutation volumeUpdateMutation($id: ID!, $input: VolumeUpdateInput!) {
    volumeUpdate(id: $id, input: $input) {
      errors {
        ... on MutationError {
          __typename
          message
          field
        }
      }
      volume {
        id
        name
        region
        type
        pathPrefix
        study {
          id
          globalId
        }
        credential {
          id
          type
          key
        }
      }
    }
  }
    """
)

delete_volume = gql(
    """
    mutation volumeDeleteMutation($id: ID!) {
      volumeDelete(id: $id) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
        volume {
          id
          name
          region
          type
          pathPrefix
        }
      }
    }
    """
)

list_and_hash = gql(
    """
      mutation volumeListAndHashMutation(
        $id: ID!
        $input: VolumeListAndHashInput!
      ) {
        volumeListAndHash(id: $id, input: $input) {
          errors {
            ... on MutationError {
              __typename
              message
              field
            }
          }
          job {
            id
            temporalWorkflowId
            operation
            target {
              id
            }
          }
        }
      }
    """
)
