class OdataAutomation:
    def __init__(self, config_yaml) -> None:
        with open(config_yaml) as f:
            self.config = yaml.load(f)

    def run_automation(self):
        # Implementation here
        return [[0, 10], [2, 30], [0, 50]]
