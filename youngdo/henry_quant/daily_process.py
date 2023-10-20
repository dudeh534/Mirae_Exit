import subprocess
"""
매일
1. kor_price
2. kor_SMA
3. kor_volume
주별
4. kor_ticker
5. kor_ticker_index
6. kor_sector
분기
7. kor_fs
8. kor_value
결과
10. main
11. day_zzang
"""
# 다른 스크립트 파일 실행
subprocess.run(["python", "kor_price.py"])
print("1. kor_price.py finish")
subprocess.run(["python", "kor_SMA.py"])
print("2. kor_SMA.py finish")
subprocess.run(["python", "kor_volume.py"])
print("3. kor_volume.py finish")
subprocess.run(["python", "henry.py"])
print("4. henry.py finish")
subprocess.run(["python", "day_zzang.py"])
print("5. day_zzang.py finish")
subprocess.run(["python", "reverse_day_zzang.py"])
print("6. reverse_day_zzang.py finish")