"""
A module for building Specimen trees and traversing the trees

Used to determine the load order of Specimens
"""

import logging
from queue import Queue
from pprint import pformat

from d3b_api_client_cli.config.concept_schema import CONCEPT

PID = CONCEPT.SAMPLE_RELATIONSHIP.PARENT.TARGET_SERVICE_ID
CID = CONCEPT.SAMPLE_RELATIONSHIP.CHILD.TARGET_SERVICE_ID

logger = logging.getLogger(__name__)


class MissingDataException(Exception):
    pass


def get_roots(relationships_df):
    """
    Get all child samples with a null parents
    These are the roots of all sample trees
    """
    return relationships_df[
        relationships_df[PID].isnull() & relationships_df[CID].notnull()
    ][CID].values.tolist()


def get_direct_children(sample_id, relationships_df):
    """
    Get the immediate/direct children of the sample identified
    by sample_id
    """
    return relationships_df[relationships_df[PID] == sample_id][
        CID
    ].values.tolist()


class TreeNode:
    """
    Represents a Specimen in a Specimen tree
    """

    def __init__(self, node_id, children=None):
        self.node_id = node_id
        self.children = None

    def level_order(self):
        """
        Traverse tree with breadth first search to output a list of tree
        nodes in level-order
        """
        output = []
        if not self.node_id:
            return output

        q = Queue()
        q.put(self)

        while not q.empty():
            current = q.get()
            output.append(current.node_id)

            if current.children:
                for c in current.children:
                    q.put(c)

        return output


def build_tree(node, relationships_df):
    """
    Given a table with directed parent->child sample relationships,
    build a list of trees representing this relationships table
    """

    children = get_direct_children(node.node_id, relationships_df)
    logger.debug(
        f"Build tree: node {node.node_id}, children {pformat(children)}"
    )
    if not children:
        return

    node.children = [TreeNode(c_id) for c_id in children]

    for c_node in node.children:
        build_tree(c_node, relationships_df)


def level_ordered_specimens(relationships_df):
    """
    For each Specimen tree, output a list of Specimens in level-order
    """
    output = []
    tree_roots = get_roots(relationships_df)

    logger.debug(f"Specimen tree roots {pformat(tree_roots)}")

    if not tree_roots:
        raise MissingDataException(
            "❌ Missing data in sample relationships table."
            " The provided sample relationships table has 0 root specimens."
            " This may because you forgot to add rows for root specimens."
            " For example if SA_1 is a root of a specimen tree then"
            " there must be a row in the sample relationshps table where"
            " child_id=SA_1 and parent_id=NULL. All root specimens require"
            " a row like this"
        )

    for root_id in get_roots(relationships_df):
        root = TreeNode(root_id)
        build_tree(root, relationships_df)
        output.extend(root.level_order())
    return output
