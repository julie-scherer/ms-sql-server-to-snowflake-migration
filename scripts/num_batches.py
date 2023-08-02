ROW_COUNTS = {
    'Contact': 22585141,
    'DatasetValue': 795207736,
    'Message': 15847177,
    'MessageDeliveryStatus': 52423259,
    'MessagePartV2': 1584766,
}

## Dictionary to store ideal batch size for specified table
#  - - - - - - - - - - - - - - - - - - - - 
BATCH_SIZES = {
    'Contact': 5000000,
    'DatasetValue': 1500000,
    'Message': 1000000,
    'MessageDeliveryStatus': 2100000,
    'MessagePartV2': 10000,
}

for (kcount,vcount), (kbatch,vbatch) in zip(ROW_COUNTS.items(), BATCH_SIZES.items()):
    # BATCH_SIZES[kbatch] / ROW_COUNTS[kbatch]
    num_batches = (vcount // vbatch) + 1
    # print(f"{kcount}:"\
    #       f"\n\t{vcount} approx rows"\
    #       f"\n\t{vbatch} records / batch"\
    #       f"\n\t= {num_batches} batches"\
    #     )
    for batch_num in range(1, num_batches+1):
        print(f"Batch {batch_num}\n")


'''
Contact: 5 batches
DatasetValue: 531
Message: 16
MessageDeliveryStatus: 25
MessagePartV2: 159

\\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\MessagePartV2
\\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\MessageDeliveryStatus
\\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Message
\\ictfs01\SharedUSA\IT\Batch\DW\BCP\CREO\Contact

'''
