s = "ttff"

def s2b( s ):
    if s.lower() == "t": return True
    else: return False

for c in s:
    print s2b( c )