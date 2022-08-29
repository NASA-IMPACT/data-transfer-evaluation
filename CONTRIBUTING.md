Strictly follow the guidelines !!
Git
* * *
1. Write quality commit messages
Use imperative, present tense for title part of your commit messages.  
For example:
Add memory efficient data structure
Instead of:
Added memory efficient data structure
Make sure these are less than or equal to 50 characters long and do not  
end with a full stop.
You may optionally add a body part, which is a paragraph with not more than  
72-characters each line and ends with a full-stop, to explain the commit  
in detail.
2. Make commits as logical change sets
Do not too many redundant commits. Do not make commits that only has un-meaningful,  
unusable or unstable changes.
Yet, make sure that your commits as separate change sets are digestible. Do not  
code for number of days and commit large independent logical changes as one commit.
3. Write description in the body
If there are many logical changes, write detailed description on the body part of the commit.  
This can be done by just using git commit command that opens up the default text editor.  
The first "non-commented" line line denotes the summary of the commit, which is generally done  
by git commit -m <commit message>. The summary is followed by an empty line and then a paragraph  
where description resides.
Have a look at this  
commit message for a reference.

Python
* * *
Strictly follow pep-8.  
Following are some of the imposed conventions that every pythonista are recommended to follow:
1. Function definitions
• Use lower case for function names
• In case the function has multiple words, separate them by underscore
• do_this()
2. Packages
• Follow same convention as in function definitions
• Be sure to create a package in a folder that contains __init__.py
• package
3. Classes
• Use UpperCaseCamelCase convention like this one
• Exception classes should end in Error
• ScrapError
• Public variables should be in lower case seperated by underscore
• self.my_list
• Private variables should begin with a single underscore
• Also, no need of setters and getters. Cheers.
4. Constants
• Use FULLY_CAPITALIZED name for constants
• In case of multiple words, separate them by underscore

Indentation and white spaces
* * *
• Do not use `tab` for indentation, convert tabs to spaces even if you use it.
• 4 spaces for indentation.
• Trim white spaces after a line of code.
• Two blank lines between import statements and other code
• Two blank lines between each function
• Add a new line before the end of file.
• SPECIAL CASE (JS, HTML, CSS)
    • JS, CSS and HTML can have many nested code by design and hence it makes sense to use 2 spaces instead of 4 for indentation.
    • Use the following eslint configuration
    {
    "parserOptions": {
        "ecmaVersion": 6,
        "sourceType": "module",
        "ecmaFeatures": {
            "jsx": true
        }
    },
    "extends": "airbnb",
    "rules": {
        "react/jsx-filename-extension": [1, { "extensions": [".js", ".jsx"] }],
        "implicit-arrow-linebreak": ["error", "below"],
        "jsx-a11y/label-has-associated-control": "off",
        "no-alert": "off",
        "import/no-extraneous-dependencies": "off"
    },
    "parser": "babel-eslint",
    "env": {
        "browser": true,
        "node": true
    },
    "globals": {
        "toastr": "readonly"
    }
    }

Readability
* * *
• Use 90 char limit for code
• At most 15-20 lines in a method, if anything goes beyond this, break it into multiple methods.
• Do not go beyond 2 levels of indentation in an if statement, if you have to, there should be a different approach to handle this.
• At most 100 lines for a file (except tests, they can get long).



