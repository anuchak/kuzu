user_ids = set()
# open ratings.csv
with open('ratings.csv', 'r') as f:
    # skip the first line
    f.readline()
    # for each line in the file
    for line in f:
        # split the line by comma
        line_split = line.split(',')
        user_id = int(line_split[0])
        if user_id not in user_ids:
            print(user_id)
            user_ids.add(user_id)

# open users.csv
with open('users.csv', 'w') as f:
    # write the header
    f.write('userId\r\n')
    # for each user id
    for user_id in user_ids:
        # write the user id to a new line to the file
        f.write(str(user_id) + '\r\n')
    # write a newline
    f.write('\r\n')