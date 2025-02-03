"""
Dewrangle GraphQL query definitions
"""

from gql import gql

volume = gql(
    """
    query volumeQuery($id: ID!) {
      node(id: $id) {
        id
        ... on Volume {
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

study_volumes = gql(
    """
    query studyVolumes($id: ID!, $first: Int, $after: ID) {
      node(id: $id) {
        id
        ... on Study {
          name
          id
          globalId
          volumes(first: $first, after: $after) {
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
        }
      }
    }
  """
)
