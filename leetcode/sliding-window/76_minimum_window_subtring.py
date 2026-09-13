class Solution:
    def minWindow(self, s: str, t: str) -> str:
        counter = defaultdict(int)
        for ch in t:
            counter[ch] += 1

        minLen = float("inf")
        resStart = 0
        start, end = 0, 0
        formed = 0
        while end < len(s):
            if s[end] in counter:
                counter[s[end]] -= 1
                if counter[s[end]] == 0:
                    formed += 1

            while formed == len(counter):
                if end - start + 1 < minLen:
                    minLen = end - start + 1
                    resStart = start
                if s[start] in counter:
                    if counter[s[start]] == 0:
                        formed -= 1
                    counter[s[start]] += 1
                start += 1
            end += 1

        return "" if minLen == float("inf") else s[resStart : resStart + minLen]
