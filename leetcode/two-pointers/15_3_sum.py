class Solution:
    def threeSum(self, nums: list[int]) -> list[list[int]]:
        res = []
        nums.sort()
        for p1 in range(len(nums) - 2):
            if p1 == 0 or nums[p1] != nums[p1 - 1]:
                p2, p3 = p1 + 1, len(nums) - 1
                while p2 < p3:
                    s = nums[p1] + nums[p2] + nums[p3]
                    if s == 0:
                        res.append([nums[p1], nums[p2], nums[p3]])
                        while p2 < p3 and nums[p2] == nums[p2 + 1]:
                            p2 += 1
                        while p2 < p3 and nums[p3] == nums[p3 - 1]:
                            p3 -= 1
                        p2 += 1
                        p3 -= 1
                    elif s < 0:
                        p2 += 1
                    else:
                        p3 -= 1

        return res
