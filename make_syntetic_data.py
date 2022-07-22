# Databricks notebook source
# MAGIC %fs
# MAGIC ls /Users/nikola.dyundev@databricks.com/complexUDF_data

# COMMAND ----------

# MAGIC %fs
# MAGIC head /Users/nikola.dyundev@databricks.com/complexUDF_data/data1.txt

# COMMAND ----------

f=open("/mnt/guru99.txt", "w")
fat_word="älskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkklaksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkhjsälskdjalksdjalskdjalksjdalksjdalksjdlkasjdklajsdklajsdklajskldajslkdjalksdjalksjdlkasjdklasjdlkajsdlkajsdlkasjdklasjdlkajsdklajskldajslkdjalkk;laksdjfakdskjadskakjalfskaljhsfdkajsdkjfdskjsdlkjsfadjksajkakjjkhkjhkjhakjhkjhkjhhjkhjkhjkjhkhjsadkjhakfsjdhkjsafhdkjhsdfakhjskhjksjhaskjskajhkashjdkhjaskjhfkfhjdskhjfsdhkjsfhkjsdfakhjfsadkhjskhjfdskhjfdskjhfsdkhjsfdakhjsfdakhjfskadhjksafhjkhjfsakhjsfakhjsfadkhjfsakhjfsadkhjfsdkhjfsdakhjksfahjkdfsahjkh"

the_fattest_word=""
for i in range(1024*100):
  f.write(fat_word)
f.close()

# COMMAND ----------

the_fattest_word=""
f2=open("/mnt/guru100.txt", "w")
f=open("/mnt/guru99.txt", "r")
for l in f:
  the_fattest_word=the_fattest_word+l
  
f2.write(the_fattest_word)
f.close()
f2.close()


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Users/nikola.dyundev@databricks.com/complexUDF_data