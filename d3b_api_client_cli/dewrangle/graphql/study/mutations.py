"""
Dewrangle GraphQL mutation definitions
"""

from gql import gql


create_study = gql(
    """
    mutation studyCreateMutation($input: StudyCreateInput!) {
      studyCreate(input: $input) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
        study {
          id
          name
          globalId
        }
      }
    }
    """
)
update_study = gql(
    """
    mutation studyUpdateMutation($id: ID!, $input: StudyUpdateInput!) {
      studyUpdate(id: $id, input: $input) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
        study {
          id
          name
          globalId
        }
      }
    }
    """
)

delete_study = gql(
    """
    mutation studyDeleteMutation($id: ID!) {
      studyDelete(id: $id) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
        study {
          id
          name
          globalId
        }
      }
    }
    """
)
