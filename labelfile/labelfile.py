import boto3
import os
import tcga_subtype_division as tsd
import csv


def filtrar_lista(list_1, list_2):
    """
    Filters list_1 by keeping only the elements that contain any substring from list_2.

    Parameters:
    -----------
    list_1 : list
        The list of strings to be filtered.

    list_2 : list
        The list of substrings to check within the elements of list_1.

    Returns:
    --------
    filtered_list : list
        A new list containing elements from list_1 that contain any substring from list_2.
    """
    filtered_list = [
        item for item in list_1 if any(substring in item for substring in list_2)
    ]
    return filtered_list


def create_csv_classes(file_name: str, *class_lists):
    """
    Creates a CSV file with two columns: 'image_path' and 'label'. Each row corresponds to an item from
    the provided class lists, along with its associated class index.

    Parameters:
    -----------
    file_name : str
        The name of the CSV file to be created.

    class_lists : list of lists
        An arbitrary number of lists, each containing strings that represent items of a class.

    Returns:
    --------
    None

    The function writes the CSV file to disk.
    """
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write the header row
        writer.writerow(['image_path', 'label'])

        # Iterate over the class lists and write each item with its class index
        for class_index, class_list in enumerate(class_lists):
            for item in class_list:
                if class_list == 0:
                    writer.writerow([item, 'cin'])
                elif class_list == 1:
                    writer.writerow([item, 'ebv'])
                elif class_list == 2:
                    writer.writerow([item, 'gs'])
                elif class_list == 3:
                    writer.writerow([item, 'msi'])

    print(f"Arquivo {file_name} criado com sucesso.")


def list_files_from_s3(bucket_name: str) -> list:
    """
    Retrieves a list of file keys from an S3 bucket, filtering out files smaller than 1MB.

    Parameters:
    -----------
    bucket_name : str
        The name of the S3 bucket to list objects from.

    Returns:
    --------
    label_list : list
        A list of object keys from the S3 bucket that are larger than 1MB.
    """
    label_list = []
    s3_client = boto3.client('s3')

    response = s3_client.list_objects_v2(Bucket=bucket_name)

    for obj in response.get('Contents', []):
        if obj['Size'] > 1_000_000:  # File size greater than 1MB
            label_list.append(obj['Key'])

    return label_list


def from_txt_to_list(file_name: str) -> list:
    """
    Reads a text file and returns a list of its lines, stripped of leading and trailing whitespace.

    Parameters:
    -----------
    file_name : str
        The name of the text file to read.

    Returns:
    --------
    list_295 : list
        A list of strings, each representing a line from the text file.
    """
    with open(file_name, 'r') as file:
        content = file.read()

    # Split the content into lines and strip whitespace
    list_295 = [line.strip() for line in content.splitlines()]

    return list_295


def generate_label_file(bucket_name: str, file_name: str, table_name: str) -> None:
    """
    Generates a CSV file containing file paths and their corresponding class labels by processing
    data from an S3 bucket, a text file, and a database table.

    Parameters:
    -----------
    bucket_name : str
        The name of the S3 bucket containing the files.

    file_name : str
        The name of the text file containing a list of identifiers to filter the files.

    table_name : str
        The name of the database table to retrieve subtype classifications from.

    Returns:
    --------
    None

    The function performs the following steps:
    1. Retrieves a list of file keys from the S3 bucket, filtering out files smaller than 1MB.
    2. Reads identifiers from the text file to create a filter list.
    3. Filters the S3 file list to include only those that match the identifiers.
    4. Further filters the files to include only those with '01A' at positions 26 to 29 in their names.
    5. Divides the filtered file list into subtypes based on classifications from the database table.
    6. Creates a CSV file 'label_file.csv' with the file paths and their corresponding class labels.
    """
    # Step 1: List files from the S3 bucket
    label_list = list_files_from_s3(bucket_name)

    # Step 2: Read identifiers from the text file
    list_295 = from_txt_to_list(file_name)

    # Step 3: Filter the S3 files with the identifiers
    filtered_list = filtrar_lista(label_list, list_295)

    # Step 4: Further filter files with '01A' at specific positions in their names
    list_01A = [item for item in filtered_list if item[26:29] == '01A']

    # Step 5: Retrieve subtype lists from the database table
    CIN_list, EBV_list, GS_list, MSI_list, POLE_list = tsd.patient_subtype_division(table_name)

    # Filter the '01A' files into their respective subtypes
    CIN_files = filtrar_lista(list_01A, CIN_list)
    EBV_files = filtrar_lista(list_01A, EBV_list)
    GS_files = filtrar_lista(list_01A, GS_list)
    MSI_files = filtrar_lista(list_01A, MSI_list)
    # Note: POLE_list is retrieved but not used in the original code

    # Step 6: Create the CSV file with the classified file paths
    criar_csv_classes('label_file.csv', CIN_files, EBV_files, GS_files, MSI_files)