import os
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "Data")

DEPARTMENTS = [
    (1, "produce"),
    (2, "dairy eggs"),
    (3, "snacks"),
    (4, "beverages"),
    (5, "frozen"),
    (6, "bakery"),
    (7, "meat seafood"),
    (8, "pantry"),
    (9, "deli"),
    (10, "personal care"),
]

PRODUCT_NAMES = {
    "produce": [
        "Organic Bananas", "Baby Spinach", "Roma Tomatoes", "Avocados", "Green Onions",
        "Strawberries", "Blueberries", "Broccoli Crown", "Russet Potatoes", "Yellow Onions",
        "Red Bell Peppers", "Cucumber", "Garlic Bulb", "Lemon", "Lime",
        "Gala Apples", "Navel Oranges", "Grape Tomatoes", "Celery", "Carrots",
    ],
    "dairy eggs": [
        "Whole Milk", "2% Reduced Fat Milk", "Large Eggs", "Salted Butter", "Greek Yogurt",
        "Cheddar Cheese", "Mozzarella Shredded", "Cream Cheese", "Sour Cream", "Heavy Whipping Cream",
        "Parmesan Cheese", "Swiss Cheese", "Cottage Cheese", "Almond Milk", "Oat Milk",
        "Egg Whites", "String Cheese", "Provolone Slices", "American Cheese", "Whipped Butter",
    ],
    "snacks": [
        "Lay's Classic Chips", "Doritos Nacho Cheese", "Cheez-It Crackers", "Oreo Cookies",
        "Peanut Butter Crackers", "Mixed Nuts", "Granola Bars", "Popcorn", "Pretzels",
        "Rice Cakes", "Fruit Snacks", "Trail Mix", "Chex Mix", "Animal Crackers",
        "Nutri-Grain Bars", "Goldfish Crackers", "Dark Chocolate Bar", "Gummy Bears",
        "Kettle Chips", "Veggie Straws",
    ],
    "beverages": [
        "Spring Water 24pk", "Sparkling Water", "Orange Juice", "Apple Juice", "Coca-Cola 12pk",
        "Diet Coke 12pk", "Sprite 12pk", "Coffee Grounds", "Green Tea Bags", "Black Tea Bags",
        "Lemonade", "Cranberry Juice", "Iced Tea", "Sports Drink", "Energy Drink",
        "Coconut Water", "Cold Brew Coffee", "Aloe Vera Drink", "Tomato Juice", "Kombucha",
    ],
    "frozen": [
        "Frozen Broccoli", "Frozen Mixed Vegetables", "Frozen Pizza", "Chicken Nuggets",
        "Fish Sticks", "Breakfast Burritos", "Frozen Waffles", "Ice Cream Vanilla",
        "Frozen Edamame", "Frozen Mac and Cheese", "Tater Tots", "Frozen Lasagna",
        "Mozzarella Sticks", "Frozen Strawberries", "Frozen Corn", "Veggie Burgers",
        "Frozen Burritos", "Pot Pies", "Frozen Shrimp", "Ice Cream Sandwich",
    ],
    "bakery": [
        "White Sandwich Bread", "Whole Wheat Bread", "Sourdough Loaf", "Bagels",
        "English Muffins", "Flour Tortillas", "Corn Tortillas", "Croissants",
        "Dinner Rolls", "Cinnamon Raisin Bread", "Baguette", "Blueberry Muffins",
        "Banana Bread", "Pita Bread", "Hamburger Buns", "Hot Dog Buns",
        "Everything Bagels", "Chocolate Chip Muffins", "Rye Bread", "Pretzel Rolls",
    ],
    "meat seafood": [
        "Boneless Chicken Breast", "Ground Beef 80/20", "Salmon Fillet", "Tilapia Fillet",
        "Pork Chops", "Baby Back Ribs", "Shrimp Large", "Ground Turkey",
        "Chicken Thighs", "Beef Steak Sirloin", "Bacon", "Italian Sausage",
        "Chicken Drumsticks", "Tuna Steaks", "Hot Dogs", "Deli Turkey Breast",
        "Ham Slices", "Lamb Chops", "Crab Legs", "Lobster Tail",
    ],
    "pantry": [
        "Extra Virgin Olive Oil", "Vegetable Oil", "All Purpose Flour", "White Sugar",
        "Brown Sugar", "Sea Salt", "Black Pepper", "Chicken Broth", "Vegetable Broth",
        "Canned Tomatoes", "Tomato Paste", "Pasta Penne", "Spaghetti", "White Rice",
        "Brown Rice", "Canned Chickpeas", "Canned Black Beans", "Peanut Butter",
        "Strawberry Jam", "Maple Syrup",
    ],
    "deli": [
        "Ham Deli Slices", "Turkey Deli Slices", "Roast Beef Slices", "Salami",
        "Pepperoni", "Chicken Caesar Salad", "Macaroni Salad", "Coleslaw",
        "Potato Salad", "Hummus", "Guacamole", "Fresh Salsa", "Pimento Cheese",
        "Prosciutto", "Capicola", "Mortadella", "Pastrami", "Corned Beef",
        "Chicken Salad", "Tuna Salad",
    ],
    "personal care": [
        "Shampoo", "Conditioner", "Body Wash", "Bar Soap", "Toothpaste",
        "Toothbrush", "Dental Floss", "Deodorant Stick", "Face Moisturizer", "Sunscreen SPF50",
        "Razors", "Shaving Cream", "Hand Lotion", "Lip Balm", "Nail Clippers",
        "Cotton Swabs", "Cotton Balls", "Feminine Pads", "Tampons", "Tissue Box",
    ],
}

MAY_START    = datetime(2025, 5, 1, 0, 0, 0)
MAY_END      = datetime(2025, 5, 31, 23, 59, 59)
FUTURE_START = datetime(2027, 1, 1, 0, 0, 0)
FUTURE_END   = datetime(2027, 12, 31, 23, 59, 59)

TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT      = "%Y-%m-%d"
