def display_row_values(df, columns=None):
    # Si aucune colonne n'est spécifiée, afficher toutes les colonnes
    if columns is None:
        columns = df.columns.tolist()

    if isinstance(columns, str) or isinstance(columns, tuple):
        columns = [columns]

    # Conversion des noms de colonnes en string (utile pour les tuples)
    columns_str = [str(col) for col in columns]

    # Calculer la largeur maximale pour chaque colonne spécifiée
    column_widths = [
        max(df[col].astype(str).apply(len).max(), len(str(col))) for col in columns
    ]

    # Afficher les noms des colonnes, formatés
    column_headers = [str(col).ljust(column_widths[i]) for i, col in enumerate(columns)]
    print("  |  ".join(column_headers))
    print("-" * (sum(column_widths) + (len(columns) - 1) * 4))  # Ligne de séparation

    # Afficher les 10 premières lignes
    for i in range(min(10, len(df))):
        row_values = [
            str(df.iloc[i, df.columns.get_loc(col)]).ljust(column_widths[j])
            for j, col in enumerate(columns)
        ]
        print("  |  ".join(row_values))

def clean_column_names(df):
    try:
        rename_dict = {
            'Close': 'Close Price',
            'High': 'High Price',
            'Low': 'Low Price',
            'Open': 'Open Price',
            'Volume': 'Volume',
            'previousClose': 'Previous Close',
            'regularMarketPreviousClose': 'Regular Market Previous Close',
            'regularMarketOpen': 'Regular Market Open',
            'regularMarketDayLow': 'Regular Market Day Low',
            'regularMarketDayHigh': 'Regular Market Day High',
            'payoutRatio': 'Payout Ratio',
            'beta': 'Beta',
            'trailingPE': 'Trailing P/E',
            'volume': 'Volume',
            'regularMarketVolume': 'Regular Market Volume',
            'marketCap': 'Market Cap',
            'fiftyTwoWeekLow': '52 Week Low',
            'fiftyTwoWeekHigh': '52 Week High',
            'priceToSalesTrailing12Months': 'Price to Sales (Last 12 Months)',
            'fiftyDayAverage': '50 Day Average',
            'twoHundredDayAverage': '200 Day Average',
            'trailingAnnualDividendRate': 'Trailing Annual Dividend Rate',
            'trailingAnnualDividendYield': 'Trailing Annual Dividend Yield',
            'currency': 'Currency',
            'enterpriseValue': 'Enterprise Value',
            'profitMargins': 'Profit Margins',
            'floatShares': 'Float Shares',
            'sharesOutstanding': 'Shares Outstanding',
            'bookValue': 'Book Value',
            'priceToBook': 'Price to Book',
            'mostRecentQuarter': 'Most Recent Quarter',
            'earningsQuarterlyGrowth': 'Earnings Quarterly Growth',
            'netIncomeToCommon': 'Net Income to Common',
            'trailingEps': 'Trailing EPS',
            'enterpriseToRevenue': 'Enterprise to Revenue',
            'enterpriseToEbitda': 'Enterprise to EBITDA',
            '52WeekChange': '52 Week Change',
            'currentPrice': 'Current Price',
            'totalCash': 'Total Cash',
            'totalDebt': 'Total Debt',
            'quickRatio': 'Quick Ratio',
            'currentRatio': 'Current Ratio',
            'totalRevenue': 'Total Revenue',
            'debtToEquity': 'Debt to Equity',
            'revenuePerShare': 'Revenue per Share',
            'returnOnAssets': 'Return on Assets',
            'returnOnEquity': 'Return on Equity',
            'grossProfits': 'Gross Profits',
            'freeCashflow': 'Free Cash Flow',
            'operatingCashflow': 'Operating Cash Flow',
            'earningsGrowth': 'Earnings Growth',
            'revenueGrowth': 'Revenue Growth',
            'grossMargins': 'Gross Margins',
            'ebitdaMargins': 'EBITDA Margins',
            'operatingMargins': 'Operating Margins',
            'exchange': 'Exchange',
            'fiftyTwoWeekLowChange': '52 Week Low Change',
            'fiftyTwoWeekLowChangePercent': '52 Week Low Change Percent',
            'fiftyTwoWeekRange': '52 Week Range',
            'fiftyTwoWeekHighChange': '52 Week High Change',
            'fiftyTwoWeekHighChangePercent': '52 Week High Change Percent',
            'fiftyTwoWeekChangePercent': '52 Week Change Percent',
            'averageAnalystRating': 'Average Analyst Rating'
        }

        new_columns = []
        for col in df.columns:
            if isinstance(col, tuple):
                # For tuple columns like ('Close', 'TSLA') -> 'Close'
                new_columns.append(col[0])
            elif col in rename_dict:
                # Use rename_dict for known fields
                new_columns.append(rename_dict[col])
            else:
                # Fallback: format unknown names nicely
                new_columns.append(col.replace('_', ' ').title())

        df.columns = new_columns
        return df
    
    except Exception as e:
        print(f"Error:{e}")
        return df

def create_table_in_mysql(df: pd.DataFrame, table_name: str, engine):
    # Vérifier si la table existe déjà
    check_table_sql = f"SHOW TABLES LIKE '{table_name}';"

    try:
        with engine.connect() as connection:
            result = connection.execute(text(check_table_sql))
            if result.fetchone():
                print(f"⚠️ La table '{table_name}' existe déjà dans MySQL.")
            else:
                # Définition des colonnes
                column_definitions = []
                for column_name, dtype in df.dtypes.items():
                    if dtype == 'object': 
                        column_type = "VARCHAR(255)"
                    elif dtype == 'int64':  
                        column_type = "INT"
                    elif dtype == 'float64': 
                        column_type = "FLOAT"
                    elif dtype == 'datetime64[ns]': 
                        column_type = "DATETIME"
                    elif dtype == 'datetime64[ns, UTC]': 
                        column_type = "DATETIME"
                    elif dtype == 'timedelta64[ns]':  
                        column_type = "TIME"
                    elif dtype == 'bool':  
                        column_type = "BOOLEAN"
                    elif dtype == 'time64[ns]': 
                        column_type = "TIME"
                    else:
                        column_type = "VARCHAR(255)" 

                    column_definitions.append(f"`{column_name}` {column_type}")

                # Création de la table
                create_table_sql = f"CREATE TABLE `{table_name}` ({', '.join(column_definitions)});"
                connection.execute(text(create_table_sql))
                print(f"✅ Table '{table_name}' créée avec succès dans MySQL.")
            
            # Charger les données sans doublons
            # Vérifier les doublons
            for index, row in df.iterrows():
                # Créer une requête d'insertion
                insert_sql = f"INSERT INTO `{table_name}` ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))});"

                # Vérifier si la ligne existe déjà dans la table
                check_duplicate_sql = f"SELECT COUNT(*) FROM `{table_name}` WHERE "
                check_duplicate_conditions = []
                for column in df.columns:
                    check_duplicate_conditions.append(f"`{column}` = %s")
                check_duplicate_sql += " AND ".join(check_duplicate_conditions)

                # Exécuter la vérification des doublons
                duplicate_check_result = connection.execute(text(check_duplicate_sql), tuple(row))
                duplicate_count = duplicate_check_result.fetchone()[0]

                # Si la ligne n'existe pas déjà, l'insérer
                if duplicate_count == 0:
                    connection.execute(text(insert_sql), tuple(row))
                    print(f"✅ Ligne insérée dans la table '{table_name}'.")
                else:
                    print(f"⚠️ Doublon détecté pour la ligne : {row}. Aucune insertion effectuée.")

    except Exception as e:
        print(f"❌ Erreur lors de la création de la table : {e}")





















