class Solution:
    def characterReplacement(self, s: str, k: int) -> int:
        maxLength = 0
        maxOccurr = 0
        charCounter = defaultdict(int)
        start, end = 0, 0
        while end < len(s):
            charCounter[s[end]] += 1
            maxOccurr = max(maxOccurr, charCounter[s[end]])
            if end - start + 1 - maxOccurr > k:
                charCounter[s[start]] -= 1
                start += 1
            maxLength = max(maxLength, end - start + 1)
            end += 1

        return maxLength
