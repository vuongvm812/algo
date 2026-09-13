class Solution:
    def lengthOfLongestSubstring(self, s: str) -> int:
        visited = set()
        maxLength = 0
        start, end = 0, 0
        while end < len(s):
            while s[end] in visited:
                visited.remove(s[start])
                start += 1
            visited.add(s[end])
            maxLength = max(maxLength, end - start + 1)
            end += 1

        return maxLength
