# strategy class
# this is a base class for strategies
# it requires taking in the 
class Strategy:
    def __init__(self, context):
        self.context = context
    
    # strategy construction rules, either indexing, factors, asset allocation etc.
    def process(self):
        pass