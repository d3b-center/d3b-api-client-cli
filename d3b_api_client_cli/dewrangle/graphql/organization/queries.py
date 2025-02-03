"""
Dewrangle GraphQL query definitions
"""

from gql import gql

organization_users = gql(
    """
    query($first: Int, $after: ID) {
      viewer {
        name
        organizationUsers(first: $first, after: $after) {
          totalCount
          pageInfo {
            hasNextPage
            endCursor
          }
          edges {
            cursor
            node {
              organization {
                studies {
                  totalCount
                }
                id
                name
                description
                email
                website
              }
            }
          }
        }
      }
}
    """
)
