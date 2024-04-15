# Yield successive n-sized
# chunks from l.
def chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]