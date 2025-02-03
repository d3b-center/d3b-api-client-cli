"""
Dewrangle GraphQL mutation definitions
"""

from gql import gql


create_billing_group = gql(
    """
    mutation billingGroupCreateMutation($input: BillingGroupCreateInput!) {
      billingGroupCreate(input: $input) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
        billingGroup {
          id
          name
          cavaticaBillingGroupId
        }
      }
    }
    """
)

delete_billing_group = gql(
    """
    mutation billingGroupDeleteMutation($id: ID!) {
      billingGroupDelete(id: $id) {
        errors {
          ... on MutationError {
            __typename
            message
            field
          }
        }
        billingGroup {
          id
          name
          cavaticaBillingGroupId
        }
      }
    }
    """
)
