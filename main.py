from src.delegators.app_delegator import App

def main():
    # App() \
    # .fetch_markets()
    App() \
        .get_markets() \
        .upsert_data()

if __name__ == '__main__':
    main()
