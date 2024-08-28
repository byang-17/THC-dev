# orchestrator/driver class
# handles the association between context and strategy,
# orchestrate the strategy creation
class PortfolioManager:
    def __init__(self, context, strategy):
        self.context = context
        self.strategy = strategy(self.context)

    def run(self):
        self.strategy.process()