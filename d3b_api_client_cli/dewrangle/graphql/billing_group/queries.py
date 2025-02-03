"""
Dewrangle GraphQL query definitions
"""

from gql import gql

billing_group = gql(
    """
    query BillingGroupQuery($id: ID!) {
      node(id: $id) {
        id
        ... on BillingGroup {
          cavaticaBillingGroupId
          name
          id
          organization {
            name
            id
          }
        }
      }
    }
    """
)

org_billing_groups = gql(
    """
    query orgBillingGroups($id: ID!, $first: Int, $after: ID) {
      node(id: $id) {
        id
        ... on Organization {
          name
          id
          billingGroups(first: $first, after: $after) {
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
                cavaticaBillingGroupId
                name
              }
            }
          }
        }
      }
    }
  """
)
