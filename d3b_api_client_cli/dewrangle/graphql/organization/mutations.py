"""
Dewrangle GraphQL mutation definitions
"""

from gql import gql

create_organization = gql(
    """
    mutation organizationCreateMutation($input: OrganizationCreateInput!) {
    organizationCreate(input: $input) {
      errors {
        ... on MutationError {
          __typename
          message
          field
        }
      }
      organization {
        id
        name
        visibility
        description
        website
        email
      }
    }
  }
  """
)
update_organization = gql(
    """
    mutation organizationUpdateMutation(
      $id: ID!
      $input: OrganizationUpdateInput!
    ) {
      organizationUpdate(id: $id, input: $input) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
        organization {
          id
          name
          visibility
          description
          website
          email
        }
      }
    }
  """
)

delete_organization = gql(
    """
    mutation organizationDeleteMutation($id: ID!) {
    organizationDelete(id: $id) {
      errors {
        ... on MutationError {
          __typename
          message
          field
        }
      }
      organization {
        id
        name
        description
        website
        email
      }
    }
  }
  """
)
