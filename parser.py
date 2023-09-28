from pyparsing import *
from firebase_query import process_input


# parser function will take in user_input and output
def parser(user_input):
    # list of expected values
    summit_heights = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000]
    locations = ["utah", "colorado", "vermont", "california", "montana", "ontario", "michigan", "washington",
                 "british-colombia", "wyoming", "alberta", "new-hampshire", "oregon", "maine", "idaho", "new-mexico",
                 "quebec", "west-virginia", "new-york"]
    resorts = ["Windham Mountain", "The Highlands", "Snowshoe", "Summit at Snoqualmie", "Winter Park", "Tremblant",
               "Taos", "Sunshine Village", "Sunday River", "Sun ValleyI", "Sugarloaf", "Sugarbush", "Stratton",
               "Steamboat", "Solitude", "Snowbird", "Snowbasin", "Snow Summit", "Revelstoke", "Palisades Tahoe",
               "Mt. Norquay", "Mt. Bhelor", "Mammoth", "Loon Mountain", "Lake Louise", "Killington", "June Mountain",
               "Jackson Hole", "Eldora", "Deer Valley", "Cypress Mountain", "Crystal Mountain", "Copper Mountain",
               "Brighton", "Boyne Mountain", "Blue Mountain", "Big Sky", "Bear Mountain", "Aspen Snowmass",
               "Arapahoe Basin", "Alta", ]
    snowfalls = [0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500]
    difficulties = ["Beginner", "Intermediate", "Advanced", "Expert"]
    types = ["Groomer", "Bumps", "Chute", "Glades"]

    # Keywords
    sum = CaselessLiteral("Summit")
    r = CaselessLiteral("Resort")
    loc = CaselessLiteral("Location")
    sn = CaselessLiteral("Snowfall")
    pt = CaselessLiteral("popular trails")
    t = CaselessLiteral("type")
    h = CaselessLiteral("help")
    d = CaselessLiteral("difficulty")

    # define parser variables
    location = oneOf(locations, caseless=True)
    snow = oneOf([str(snowfall) for snowfall in snowfalls])
    summit = oneOf([str(summit_height) for summit_height in summit_heights])
    dif = oneOf(difficulties, caseless=True)
    typ = oneOf(types, caseless=True)
    resort = oneOf(resorts, caseless=True)

    # define operators in grammar
    equals_operator = Literal("==")
    greater_operator = Literal(">")
    less_operator = Literal("<")
    and_operator = CaselessLiteral("and")
    of_operator = CaselessLiteral("of")
    or_operator = CaselessLiteral("or")
    comparison_op = (greater_operator | less_operator)

    # create identifiable expressions ==
    loc_exp = (loc + equals_operator + location)("loc_exp")
    typ_exp = (t + equals_operator + typ)("typ_exp")
    diff_exp = (d + equals_operator + dif)("diff_exp")
    res_exp = (r + equals_operator + resort)("res_exp")

    # create expressions with <,>
    sum_exp = (sum + comparison_op + summit)("sum_exp")
    snow_exp = (sn + comparison_op + snow)("snow_exp")
    # create expressions with of
    pt_exp = ((pt + of_operator + resort)("pt_exp"))

    # create and_expr
    expressions_list = [res_exp, sum_exp, pt_exp, loc_exp, snow_exp, typ_exp, diff_exp]
    and_exp = (Or(expressions_list) + and_operator + Or(expressions_list))
    or_exp = (Or(expressions_list) + or_operator + Or(expressions_list))

    expression = Forward()

    # expression definition (for grammar)
    expression << (and_exp | or_exp | loc_exp | pt_exp | h | snow_exp | typ_exp | diff_exp | res_exp | sum_exp)

    # accepts strings in language and returns the parsed input
    try:
        result = expression.parseString(user_input)
        if (user_input.lower() == "help"):
            return 1
        else:
            return result
    except Exception as e:
        return 0


# main program to call parser function and firestorm function
def main():
    intro = '''Dear beloved user,
        I was built in order to help provide information on all ski resorts on the Ikon pass. 
Please excuse me if my information is not completely up to date; The data is a few years old. 
Regardless, I am overjoyed to provide my assistance. Please follow all of the syntax specifications provided below as I am very picky!
HAPPY SEARCHING!\n'''

    help_string = '''List of top level collection fields you may use in query:
    - Location: returns all resorts in specified location
    - Snowfall: returns all resorts with average seasonal snowfalls as specified (must be in a number in range 0-500 incrementing by 50)
    - Resort:   returns all top level collection information on specified resort
    - Summit:   returns all resorts with summit height as specified (must be in a number in range 1000-14000 incrementing by 100)
                
List of sub collection fields you may use in query:
    - Popular Trails of [resort]: returns the popular trails for a given resort if the sub collection 
                                  exists
    - Type:                      returns top level collection information of resorts that contain 
                                  popular trails of specified trail type 
                                  (types include “Chute”, “Glades”, “Bumps”, and “Groomer”)
    - Difficulties:              returns top level collection information of resorts with popular 
                                 trails of specified level of difficulty
                                 (difficulties include “Beginner”, “Intermediate”, “Advanced”, 
                                  and “Expert”)
                 
                
- help (returns this menu of rules)
                
                
List of operators available to use:
==, >, <, and, or
                
Important Information:
    - Commands must follow the following structure:
      Field operator argument where argument can only be one of the specified words for each field. User 
      can also perform compound commands by combining commands defined above with ‘or’ or ‘and’.
    - If the user asks for information about sub collection fields but does not include “Popular Trails” 
      in the command, it will return top level info with those types of trails
    - Snowfall and Summit must be used with the greater than or less than operators. Popular Trails must 
      be used with ‘of’ along with a resort, while the remaining fields must be used with ==.
                
Example searches could be: 
    - location == vermont and snowfall > 400
    - type == chute
    - Popular trails of Alta and difficulty == advanced
    - Snowfall > 400 and Summit > 8000'''
    print(intro)
    print(help_string)
    user_input = input("\nPlease enter your command: \n")

    while (user_input.lower() != 'quit'):
        parsed_input = parser(user_input)
        if parsed_input == 1:
            print(help_string)
            user_input = input("\nPlease enter your command: \n")
        elif parsed_input == 0:
            print("Invalid input")
            user_input = input("\nPlease enter your command: \n")
        else:

            # call the query program to filter parsed input and print the results
            output = process_input(parsed_input)
            output_list = []
            # determine if th e output is a list or a stream of query results
            if isinstance(output, list):
                output_list = output
            else:
                for doc in output:
                    output_list.append(doc.to_dict())
            # add space between user input and output
            print("\n")
            # determine if the list is empty
            if len(output_list) == 0:
                print("No information available")
            # iterate through the output list
            for dictionary in output_list:
                # iterate through the list to print the output
                for key, value in dictionary.items():
                    print(f"{key}: {value}")
                print("---------------------------")

            user_input = input("Please enter your command: \n")

    print('Thank you!')


main()
