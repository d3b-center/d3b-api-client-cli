"""
Dewrangle GraphQL mutation definitions
"""

from gql import gql

fhir_resource_ingest = gql(
    """
    mutation fhirResourceIngestMutation(
      $input: FhirResourceIngestInput!
    ) {
      fhirResourceIngest(input: $input) {
        job {
          id
        }
        errors {
          ... on MutationError {
            message
            field
          }
        }
      }
    }
    """
)
