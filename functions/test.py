import pandas as pd


def process_columns_with_colon(df, columns_to_process):
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
        df[column] = df[column].apply(lambda x: x.split(": ")[1] if ":" in x else x)

    return df


# Example usage:
# Create a sample DataFrame
data = {
    "Gender": ["F: Female", "M: Male", "_T: Total"],
    "Category": ["A: Category1", "B: Category2", "C: Category3"],
    "Category2": ["Category1_New", "Category2_New", "Category3_New"],
}
df = pd.DataFrame(data)

# Columns to process
columns_to_process = ["Gender", "Category", "Category2"]

# Call the function to process columns
df = process_columns_with_colon(df, columns_to_process)

# Print the DataFrame with processed columns
print(df)
