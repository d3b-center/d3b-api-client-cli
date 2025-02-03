"""
Dewrangle GraphQL query definitions
"""

from gql import gql

credential = gql(
    """
    query credentialQuery($id: ID!) {
      node(id: $id) {
        id
        ... on Credential {
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

study_credentials = gql(
    """
    query studyCredentials($id: ID!, $first: Int, $after: ID) {
      node(id: $id) {
        id
        ... on Study {
          name
          id
          globalId
          credentials(first: $first, after: $after) {
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
                name
                key
              }
            }
          }
        }
      }
    }
  """
)
