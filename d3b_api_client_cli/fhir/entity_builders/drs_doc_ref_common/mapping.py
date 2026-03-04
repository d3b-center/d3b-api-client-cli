# noqa

"""
Map a string from d3b_api_client_cli.fhir.constants to 
a FHIR coding type
"""

from d3b_api_client_cli.fhir import constants

# https://includedcc.org/fhir/code-systems/data_types
type_coding = {
    # Non-index file data types
    "Aligned Read": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads",
        "display": "Aligned Reads",
    },
    constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads",
        "display": "Aligned Reads",
    },
    constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads-Index",
        "display": "Aligned Reads Index",
    },
    "Alternative Splicing": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Alternative-Splicing",
        "display": "Alternative Splicing",
    },
    "Annotated Gene Fusion": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Annotated-Gene-Fusion",
        "display": "Annotated Gene Fusion",
    },
    "Annotated Germline Structural Variation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Germline-Structural-Variations",
        "display": "Germline Structural Variations",
    },
    "Annotated Somatic Copy Number Segment": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Copy-Number-Variations",
        "display": "Somatic Copy Number Variations",
    },
    "Annotated Somatic Mutation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations",
        "display": "Simple Nucleotide Variations",
    },
    "Annotated Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations-Index",
        "display": "Simple Nucleotide Variations Index",
    },
    "Annotated Somatic Mutations": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations",
        "display": "Simple Nucleotide Variations",
    },
    "Annotated Variant Call": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations",
        "display": "Simple Nucleotide Variations",
    },
    "Annotated Variant Call Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations-Index",
        "display": "Simple Nucleotide Variations Index",
    },
    "Consensus Somatic Mutation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations",
        "display": "Simple Nucleotide Variations",
    },
    "Consensus Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations-Index",
        "display": "Simple Nucleotide Variations Index",
    },
    constants.GENOMIC_FILE.DATA_TYPE.EXPRESSION: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Gene-Expression-Quantifications",
        "display": "Gene Expression Quantifications",
    },
    "Extra-Chromosomal DNA": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Extra-Chromosomal-DNA",
        "display": "Extra-Chromosomal DNA",
    },
    "Familial Relationship Report": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Familial-Relationship-Report",
        "display": "Familial Relationship Report",
    },
    "Gene Expression": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Gene-Expression-Quantifications",
        "display": "Gene Expression Quantifications",
    },
    "Gene Expression Quantification": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Gene-Expression-Quantifications",
        "display": "Gene Expression Quantifications",
    },
    constants.GENOMIC_FILE.DATA_TYPE.GENE_FUSIONS: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Gene-Fusions",
        "display": "Gene Fusions",
    },
    "Gene Level Copy Number": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Gene-Level-Copy-Number",
        "display": "Gene Level Copy Number",
    },
    "Genome Aligned Read": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads",
        "display": "Aligned Reads",
    },
    "Genome Aligned Read Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads-Index",
        "display": "Aligned Reads Index",
    },
    "Genomic Variant": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "gVCF",
        "display": "gVCF",
    },
    "Genomic Variant Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "gVCF-Index",
        "display": "gVCF Index",
    },
    constants.GENOMIC_FILE.DATA_TYPE.GVCF: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "gVCF",
        "display": "gVCF",
    },
    constants.GENOMIC_FILE.DATA_TYPE.GVCF_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "gVCF-Index",
        "display": "gVCF Index",
    },
    "Isoform Expression": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Isoform-Expression-Quantifications",
        "display": "Isoform Expression Quantifications",
    },
    "Isoform Expression Quantifications": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Isoform-Expression-Quantifications",
        "display": "Isoform Expression Quantifications",
    },
    "Masked Consensus Somatic Mutation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations",
        "display": "Somatic Simple Nucleotide Variations",
    },
    "Masked Consensus Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations-Index",
        "display": "Somatic Simple Nucleotide Variations Index",
    },
    "Masked Somatic Mutation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations",
        "display": "Somatic Simple Nucleotide Variations",
    },
    "Masked Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations-Index",
        "display": "Somatic Simple Nucleotide Variations Index",
    },
    "Pre-pass Somatic Structural Variation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations",
        "display": "Somatic Structural Variations",
    },
    "Pre-pass Somatic Structural Variation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations-Index",
        "display": "Somatic Structural Variations Index",
    },
    "Raw Gene Fusion": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Raw-Gene-Fusions",
        "display": "Raw Gene Fusions",
    },
    "Raw Germline Structural Variation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Germline-Structural-Variations",
        "display": "Germline Structural Variations",
    },
    "Raw Germline Structural Variation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Germline-Structural-Variations-Index",
        "display": "Germline Structural Variations Index",
    },
    "Raw Simple Somatic Mutation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations",
        "display": "Somatic Simple Nucleotide Variations",
    },
    "Raw Simple Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations-Index",
        "display": "Somatic Simple Nucleotide Variations Index",
    },
    "Raw Somatic Copy Number Segment": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Copy-Number-Variations",
        "display": "Somatic Copy Number Variations",
    },
    "Raw Somatic Structural Variation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations",
        "display": "Somatic Structural Variations",
    },
    "Raw Somatic Structural Variation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations-Index",
        "display": "Somatic Structural Variations Index",
    },
    "Simple Nucleotide Variations": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations",
        "display": "Somatic Simple Nucleotide Variations",
    },
    "Somatic Copy Number Variation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Copy-Number-Variations",
        "display": "Somatic Copy Number Variations",
    },
    constants.GENOMIC_FILE.DATA_TYPE.SOMATIC_COPY_NUMBER_VARIATIONS: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Copy-Number-Variations",
        "display": "Somatic Copy Number Variations",
    },
    "Somatic Structural Variation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations",
        "display": "Somatic Structural Variations",
    },
    constants.GENOMIC_FILE.DATA_TYPE.SOMATIC_STRUCTURAL_VARIATIONS: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations",
        "display": "Somatic Structural Variations",
    },
    "Transcriptome Aligned Read": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads",
        "display": "Aligned Reads",
    },
    "Tumor Copy Number Segment": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Tumor-Copy-Number-Segment",
        "display": "Tumor Copy Number Segment",
    },
    "Tumor Ploidy and Purity": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Tumor Ploidy and Purity",
        "display": "Tumor Ploidy and Purity",
    },
    "Unaligned Reads": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Unaligned-Reads",
        "display": "Unaligned Reads",
    },
    constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Variant-Calls",
        "display": "Variant Calls",
    },
    # Index File Data Types
    constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads-Index",
        "display": "Aligned Reads Index",
    },
    "Annotated Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations-Index",
        "display": "Simple Nucleotide Variations Index",
    },
    "Annotated Variant Call Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations-Index",
        "display": "Simple Nucleotide Variations Index",
    },
    "Consensus Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations-Index",
        "display": "Simple Nucleotide Variations Index",
    },
    "Genome Aligned Read Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads-Index",
        "display": "Aligned Reads Index",
    },
    "Genomic Variant Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "gVCF-Index",
        "display": "gVCF Index",
    },
    constants.GENOMIC_FILE.DATA_TYPE.GVCF_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "gVCF-Index",
        "display": "gVCF Index",
    },
    "Masked Consensus Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations-Index",
        "display": "Somatic Simple Nucleotide Variations Index",
    },
    "Masked Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations-Index",
        "display": "Somatic Simple Nucleotide Variations Index",
    },
    "Pre-pass Somatic Structural Variation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations-Index",
        "display": "Somatic Structural Variations Index",
    },
    "Raw Germline Structural Variation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Germline-Structural-Variations-Index",
        "display": "Germline Structural Variations Index",
    },
    "Raw Simple Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations-Index",
        "display": "Somatic Simple Nucleotide Variations Index",
    },
    "Raw Somatic Structural Variation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations-Index",
        "display": "Somatic Structural Variations Index",
    },
    constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Variant-Calls-Index",
        "display": "Variant Calls Index",
    },
    # Metric File types
    "HLA Genotyping": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "HLA-Genotyping",
        "display": "HLA Genotyping",
    },
    "Artifact Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Artifact-Metrics",
        "display": "Artifact Metrics",
    },
    "Cutadapter Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Cutadapter-Metrics",
        "display": "Cutadapter Metrics",
    },
    "GC Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "GC-Metrics",
        "display": "GC Metrics",
    },
    "Gender Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Gender-Metrics",
        "display": "Gender Metrics",
    },
    "Gender QC Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Gender-QC-Metrics",
        "display": "Gender QC Metrics",
    },
    "Het Call QC Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Het-Call-QC-Metrics",
        "display": "Het Call QC Metrics",
    },
    "Insert Size Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Cutadapter-Metrics",
        "display": "Cutadapter Metrics",
    },
    "QC Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "QC-Metrics",
        "display": "QC Metrics",
    },
    "Relatedness QC Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Relatedness-QC-Metrics",
        "display": "Relatedness QC Metrics",
    },
    "RNAseq Alignment Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "RNAseq-Alignment-Metrics",
        "display": "RNAseq Alignment Metrics",
    },
    "Somatic Copy Number Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Copy-Number-Metrics",
        "display": "Somatic Copy Number Metrics",
    },
    "Variant Calling Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Variant-Calling-Metrics",
        "display": "Variant Calling Metrics",
    },
    "WGS Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "WGS-Metrics",
        "display": "WGS Metrics",
    },
    "WXS Metrics": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "WXS-Metrics",
        "display": "WXS Metrics",
    },
    "Genome Aligned Reads": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Genome-Aligned-Reads",
        "display": "Genome Aligned Reads",
    },
    "Genome Aligned Reads Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Genome-Aligned-Reads-Index",
        "display": "Genome Aligned Reads Index",
    },
}

# https://includedcc.org/fhir/code-systems/experimental_strategies
experimental_strategy_coding = {
    constants.SEQUENCING.STRATEGY.LINKED_WGS: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "Linked-Read-WGS",
        "display": "Linked-Read WGS",
    },
    constants.SEQUENCING.STRATEGY.METHYL: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "Methylation",
        "display": "Methylation",
    },
    constants.SEQUENCING.STRATEGY.MRNA: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "miRNA-Seq",
        "display": "MicroRNA Sequencing",
    },
    constants.SEQUENCING.STRATEGY.RNA: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "RNA-Seq",
        "display": "RNA Sequencing",
    },
    "scRNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "scRNA-Seq",
        "display": "Single-Cell RNA Sequencing",
    },
    "snRNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "snRNA-Seq",
        "display": "Single-Nucleus RNA Sequencing",
    },
    constants.SEQUENCING.STRATEGY.TARGETED: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "Targeted-Sequencing",
        "display": "Targeted Sequencing",
    },
    constants.SEQUENCING.STRATEGY.WGS: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "WGS",
        "display": "Whole Genome Sequencing",
    },
    constants.SEQUENCING.STRATEGY.WXS: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "WXS",
        "display": "Whole Exome Sequencing",
    },
    # long reads strategies
    "ONT WGS": {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "ONT-WGS",
        "display": "ONT Whole Genome Sequencing",
    },
    "Circular Consensus Sequencing WGS": {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "CCS-WGS",
        "display": "Circular Consensus Whole Genome Sequencing",
    },
    "Circular Consensus Sequencing RNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "CCS-RNASeq",
        "display": "Circular Consensus RNA Sequencing",
    },
    "Continuous Long Reads WGS": {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "CLR-WGS",
        "display": "Continuous Long Reads Whole Genome Sequencing",
    },
    "Continuous Long Reads RNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "CLR-RNASeq",
        "display": "Continuous Long Reads RNA Sequencing",
    },
}

# https://includedcc.org/fhir/code-systems/data_categories
data_cateogry_coding = {
    constants.SEQUENCING.STRATEGY.LINKED_WGS: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    constants.SEQUENCING.STRATEGY.METHYL: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    constants.SEQUENCING.STRATEGY.MRNA: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
    "scRNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
    "snRNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
    constants.SEQUENCING.STRATEGY.RNA: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
    constants.SEQUENCING.STRATEGY.TARGETED: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    constants.SEQUENCING.STRATEGY.WGS: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    constants.SEQUENCING.STRATEGY.WXS: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    # long reads strategies
    "ONT WGS": {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    "Circular Consensus Sequencing WGS": {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    "Circular Consensus Sequencing RNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
    "Continuous Long Reads WGS": {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    "Continuous Long Reads RNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
}

# https://includedcc.org/fhir/code-systems/data_access_types
data_access_coding = {
    "true": {
        "system": "https://includedcc.org/fhir/code-systems/data_access_types",
        "code": "controlled",
        "display": "Controlled",
    },
    "false": {
        "system": "https://includedcc.org/fhir/code-systems/data_access_types",
        "code": "registered",
        "display": "Registered",
    },
    "default": {
        "system": "https://includedcc.org/fhir/code-systems/data_access_types",
        "code": "controlled",
        "display": "Controlled",
    },
}

file_format_coding = {
    "system": "https://includedcc.org/fhir/code-systems/file_formats",
}
