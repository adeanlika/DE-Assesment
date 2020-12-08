"""
compute many boxes can be made based on cakes and apple input which will be divided evenly each box
Input cakes: 20
Input apple: 25
Output : Total Number of boxes that can be made are: 5, Number of cake in each box: 4, Number of apple in each box: 5
"""

class CombineBoxes(object):
    def compute_boxes(self, cakes, apples):
        if cakes > apples:
            small = apples
        else:
            small = cakes
        for i in range(1, small+1):
            if(cakes % i == 0) and (apples % i == 0):
                gcd = i
        return gcd


if __name__ == "__main__":
    cb = CombineBoxes()

    # collecting input
    cakes = int(input("Enter Number of Cakes: "))
    apples = int(input("Enter Number of Apple: "))

    # calling function
    gcd = cb.compute_boxes(cakes, apples)

    # output
    print("Total Number of boxes that can be made are: ", gcd)
    print("Number of cake in each box: ", cakes//gcd)
    print("Number of apple in each box: ", apples//gcd)

