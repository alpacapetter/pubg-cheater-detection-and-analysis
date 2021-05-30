# The purpose of this script is to sort and convert the banned user data into a csv file
# Banned user data was collected from https://pubg.game.daum.net

with open('./kakao_pubg_banned_users_202002.txt') as banned_user_file:
    lines = banned_user_file.read()

#strip and delete empty str and user names that contains "*"
banned_user_list = ""

for line in lines:
    line = repr(line)
    line = line.replace(' ', '')
    line = line.replace("'", '')
    line = line.replace("\\n", ',')
    line = line.replace('\\t', ',')
    banned_user_list += line

banned_user_list = banned_user_list.split(',')
banned_user_list = list(filter(None, banned_user_list))
for user in banned_user_list:
    if "*" in user:
        banned_user_list.remove(user)