class Solution:
    def checkInclusion(self, s1: str, s2: str) -> bool:
        counter1 = [0] * 26
        for ch in s1:
            counter1[ord(ch) - ord("a")] += 1

        counter2 = [0] * 26
        start, end = 0, 0
        while end < len(s2):
            counter2[ord(s2[end]) - ord("a")] += 1
            containPerm = True
            for i in range(26):
                if counter1[i] != 0 and counter1[i] != counter2[i]:
                    containPerm = False
                    break
            if containPerm:
                return True

            if end - start + 1 >= len(s1):
                counter2[ord(s2[start]) - ord("a")] -= 1
                start += 1
            end += 1

        return False
