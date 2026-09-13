class Solution:
    def maxSlidingWindow(self, nums: List[int], k: int) -> List[int]:
        res = []
        store = deque()
        for i in range(len(nums)):
            if len(store) > 0 and store[0] < i - k + 1:
                store.popleft()
            while len(store) > 0 and nums[i] > nums[store[-1]]:
                store.pop()
            store.append(i)
            if i >= k - 1:
                res.append(nums[store[0]])

        return res
