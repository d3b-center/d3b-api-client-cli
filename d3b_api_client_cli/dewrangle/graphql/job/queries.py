"""
Dewrangle GraphQL query definitions
"""

from gql import gql

job = gql(
    """
    query jobQuery($id: ID!) {
      node(id: $id) {
        id
        ... on Job {
          id
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
