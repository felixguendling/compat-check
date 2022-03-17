import json

f = open('rpc_compat.json')
compat = json.load(f)

versions = compat['all_versions']
compat_table = [[True for x in range(len(versions))] for y in range(len(versions))]

for v in versions:
	print("downloading version ", v, "...")
	print("downloading corpus for version", v, "...")

for version, not_compat_list in compat['not_compatbile'].items():
	version_compat_row = compat_table[versions.index(version)
	]for not_compat in not_compat_list:
		if not_compat[0] == '>':
			until = versions.index(not_compat[1:].strip()) + 1
			for i in range(until, len(versions)):
				version_compat_row[i] = False
		elif not_compat[0] == '<':
			starting = versions.index(not_compat[1:].strip())
			for i in range(0, starting):
				version_compat_row[i] = False
		elif not_compat[0] == '+':
			add = versions.index(not_compat[1:].strip())
			version_compat_row[add] = True
		else:
			print("unkown indicator ", not_compat)

for i, compat in enumerate(compat_table):
	for j, is_compatible in enumerate(compat):
		is_upgrade = j < i
		is_downgrade = i < j
		if is_compatible:
			if is_upgrade:
				print(f"check  UPGRADE     compat between {versions[i]} and {versions[j]}: ./redpanda-{versions[i]} --read-corpus corpus/{versions[j]}")
			elif is_downgrade:
				print(f"check  DOWNGRADE   compat between {versions[i]} and {versions[j]}: ./redpanda-{versions[i]} --read-corpus corpus/{versions[j]}")
			else:
				print(f"check {versions[i]} can read it's own data ./redpanda-{versions[i]} --read-corpus corpus/{versions[j]}")
		else:
			if is_upgrade:
				print(f"check  UPGRADE   INcompat between {versions[i]} and {versions[j]}: check-throw ./redpanda-{versions[i]} --read-corpus corpus/{versions[j]}")
			elif is_downgrade:
				print(f"check  DOWNGRADE INcompat between {versions[i]} and {versions[j]}: check-throw ./redpanda-{versions[i]} --read-corpus corpus/{versions[j]}")
			else:
				print("not self-compatible?!")
	print('\n')