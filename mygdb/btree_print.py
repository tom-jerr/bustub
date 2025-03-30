import gdb
class BPlusTreePrint(gdb.Command):
    """Print B+ tree structure."""
    def __init__(self):
        super(BPlusTreePrint, self).__init__("btree_print", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        # Get the address of the B+ tree root node
        root = gdb.parse_and_eval(arg)
        # Call the recursive function to print the tree
        self.print_tree(root)

    def print_tree(self, node):
        # Check if the node is a leaf or an internal node
        if node['page_type_'] == 1:
            self.print_leaf(node)
        elif node['page_type_'] == 2:
            self.print_internal(node)

    def print_internal(self, node):
        # Print internal node keys and children
        keys = [node['key_array_'][i] for i in range(1..node['size_'])]
        children = [node['page_id_array_'][i] for i in range(node['size_'] + 1)]
        print("Internal Node: Keys:", keys)
        for child in children:
            self.print_tree(child)

    def print_leaf(self, node):
        # Print leaf node keys and values
        keys = [node['key_array_'][i] for i in range(node['size_'])]
        values = [node['rid_array_'][i] for i in range(node['size_'])]
        print("Leaf Node: Keys:", keys, "Values:", values)
# Register the command
BPlusTreePrint()

# Usage:
