import pandas as pd


class DataCleaner:
    def capitalize_first_character(
        self, df: pd.DataFrame, column: str
    ) -> [pd.DataFrame, list]:
        """
        Capitalize the first character of the values in a column of a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to perform the capitalization on.
            column (str): The name of the column to capitalize the first character.

        Returns:
            pd.DataFrame: The DataFrame with the specified column updated.

        Example:
            cleaner = DataCleaner()

            # Assuming we have a DataFrame named 'df' with a column 'Text' containing values
            df = pd.DataFrame({'Text': ['my car is red', 'i love python', 'openai is awesome']})

            # Call the function to capitalize the first character of the values in the 'Text' column
            df = cleaner.capitalize_first_character(df, 'Text')

            # Print the updated DataFrame
            print(df)
        """
        df[column] = df[column].str.capitalize()
        return df, df[column]

    def convert_to_lowercase(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Convert the text in specified columns of a DataFrame to lowercase.

        Args:
            df (pd.DataFrame): The DataFrame in which the conversion should be performed.
            columns (list): A list of column names to convert to lowercase.

        Returns:
            pd.DataFrame: The DataFrame with the specified columns converted to lowercase.

        Example:
            cleaner = DataCleaner()

            # Assuming we have a DataFrame named 'df' with columns 'Name' and 'Description'
            df = pd.DataFrame({'Name': ['John', 'Alice', 'Bob'], 'Description': ['Good', 'Bad', 'Neutral']})

            # Call the function to convert the 'Name' and 'Description' columns to lowercase
            df = cleaner.convert_to_lowercase(df, ['Name', 'Description'])

            # Print the updated DataFrame
            print(df)
        """
        df[columns] = df[columns].apply(
            lambda x: x.str.lower() if x.dtype == "object" else x
        )
        return df

    def remove_special_characters(
        self, df: pd.DataFrame, columns: list
    ) -> pd.DataFrame:
        """
        Remove special characters from specified columns of a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame in which the special characters should be removed.
            columns (list): A list of column names to remove special characters from.

        Returns:
            pd.DataFrame: The DataFrame with the specified columns having special characters removed.

        Example:
            cleaner = DataCleaner()

            # Assuming we have a DataFrame named 'df' with columns 'Text' and 'Description'
            df = pd.DataFrame({'Text': ['Hello!', '@OpenAI', '12345'], 'Description': ['Good', 'Bad', 'Neutral']})

            # Call the function to remove special characters from the 'Text' and 'Description' columns
            df = cleaner.remove_special_characters(df, ['Text', 'Description'])

            # Print the updated DataFrame
            print(df)
        """
        df[columns] = df[columns].apply(
            lambda x: x.str.replace(r"[^\w\s]", "") if x.dtype == "object" else x
        )
        return df

    def replace_text(
        self, df: pd.DataFrame, column: str, incorrect_texts: list, correct_text: str
    ) -> pd.DataFrame:
        """
        Replace incorrect text values in a specified column of a DataFrame with the correct text.

        Args:
            df (pd.DataFrame): The DataFrame in which the replacement should be performed.
            column (str): The name of the column in the DataFrame where the text should be replaced.
            incorrect_texts (list): A list of incorrect text values that need to be replaced.
            correct_text (str): The correct text value that should replace the incorrect texts.

        Example:
            cleaner = DataCleaner()

            # Assuming we have a DataFrame named 'df' with a column named 'text_column'
            df = pd.DataFrame({'text_column': ['apple', 'banna', 'aple', 'orange', 'grape']})

            # Define the incorrect texts as a list and the correct text
            incorrect_texts = ['aple', 'banna']
            correct_text = 'correct'

            # Call the function to replace the incorrect texts with the correct text in the 'text_column'
            df = cleaner.replace_text(df, 'text_column', incorrect_texts, correct_text)

            # Print the updated DataFrame

        Returns:
            pd.DataFrame: The DataFrame with the replaced text values.
        """
        df[column] = df[column].replace(incorrect_texts, correct_text)
        return df

    def replace_text_dict(
        self, df: pd.DataFrame, column: str, replacements: dict
    ) -> pd.DataFrame:
        """
        Replace wrong text in a column of a DataFrame based on a given dictionary of replacements.

        Args:
            df (pd.DataFrame): The DataFrame to perform the text replacements on.
            column (str): The name of the column to replace text in.
            replacements (dict): A dictionary containing the incorrect text as keys and the correct text as values.

        Returns:
            pd.DataFrame: The DataFrame with the specified column updated with the corrected text.

        Example:
            cleaner = DataCleaner()

            # Assuming we have a DataFrame named 'df' with a column 'Text' containing wrong text
            df = pd.DataFrame({'Text': ['aple', 'bannana', 'grapee', 'orange']})

            # Define the replacements dictionary
            replacements = {
                'aple': 'apple',
                'bannana': 'banana',
                'grapee': 'grape'
            }

            # Call the function to replace the wrong text in the 'Text' column
            df = cleaner.replace_text(df, 'Text', replacements)

            # Print the updated DataFrame
            print(df)
        """
        df[column] = df[column].replace(replacements)
        return df

    def trim_whitespaces(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Trim leading and trailing whitespaces from specified columns of a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame in which the whitespaces should be trimmed.
            columns (list): A list of column names to trim whitespaces from.

        Returns:
            pd.DataFrame: The DataFrame with trimmed whitespaces in the specified columns.

        Example:
            cleaner = DataCleaner()

            # Assuming we have a DataFrame named 'df' with columns 'Name' and 'Description'
            df = pd.DataFrame({'Name': ['   John  ', 'Alice', '  Bob   '], 'Description': [' Good', 'Bad ', ' Neutral ']})

            # Call the function to trim whitespaces from the 'Name' and 'Description' columns
            df = cleaner.trim_whitespaces(df, ['Name', 'Description'])

            # Print the updated DataFrame
            print(df)
        """
        df[columns] = df[columns].apply(
            lambda x: x.str.strip() if x.dtype == "object" else x
        )
        return df

    def rename_columns_remove_colon(df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename columns in a Pandas DataFrame by removing ":" from the column names.

        Args:
            df (pd.DataFrame): The DataFrame to rename columns for.

        Returns:
            pd.DataFrame: The DataFrame with columns renamed.

        Example:
            # Create a sample DataFrame
            data = {'FREQ: Frequency': [1, 2, 3], 'VAL: Value': [10, 20, 30]}
            df = pd.DataFrame(data)

            # Call the function to rename columns
            df = rename_columns_remove_colon(df)

            # Print the DataFrame with renamed columns
            print(df)
        """
        # Create a dictionary to store the mapping of old column names to new column names
        column_mapping = {}

        # Iterate through the existing column names
        for old_col in df.columns:
            # Split the column name at ":"
            parts = old_col.split(":")

            # If there is a ":" in the column name and there is text after it
            if len(parts) == 2:
                new_col = parts[
                    1
                ].strip()  # Use the text after ":" as the new column name
                column_mapping[old_col] = new_col

        # Rename the columns in the DataFrame using the mapping
        df.rename(columns=column_mapping, inplace=True)

        return df

    @staticmethod
    def process_columns_with_colon(
        df: pd.DataFrame, columns_to_process
    ) -> pd.DataFrame:
        """
        Process specified columns in a DataFrame by replacing values with text after ":".

        Args:
            df (pd.DataFrame): The DataFrame to process.
            columns_to_process (list): A list of column names to process.

        Returns:
            pd.DataFrame: The DataFrame with values in specified columns processed.

        Example:
            # Create a sample DataFrame
            data = {'Gender': ['F: Female', 'M: Male', '_T: Total'],
                    'Category': ['A: Category1', 'B: Category2', 'C: Category3']}
            df = pd.DataFrame(data)

            # Columns to process
            columns_to_process = ['Gender', 'Category']

            # Call the function to process columns
            df = process_columns_with_colon(df, columns_to_process)

            # Print the DataFrame with processed columns
            print(df)
        """
        for column in columns_to_process:
            # Apply a function to each cell in the specified column
            # df[column] = df[column].apply(lambda x: x.split(": ")[1] if ":" in x else x)
            df[column] = df[column].apply(
                lambda x: x.split(": ")[1]
                if (":" in x) and len(x.split(": ")) > 1
                else x
            )
        return df
