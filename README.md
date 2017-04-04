# Creating a command

The BaseScript class takes care of much of the standard dirac stuff for you, especially commandline arguments.
To extend it, create your own class which inherits from it and then:  
1. Implement a main() function to do your logic  
2. under you `if __name__ == "__main__"` block, create an instance, passing your desired arguments to the initialization method  
3. call your initialized instance

Some notes:
- If you create CLI switch with `('f', 'foo-bar', 'a fooish switch', 4)`, then the value is available within main as `self.foo_bar` (note that hypens convert to underscores).
- You may create your own setter method for your switch, it must be named `set_<your_switch_name>` (again with hyphens in the switch converted to underscores). It may then implement whatever logic or validity checks you need. The value can be stored in whatever member variable you choose, but `self.<your_switch_name>` is standard.
- Any DIRAC modules you need should be imported inside your implementation of *main*, this ensures that they happen after required API interactions have happened
- You should include the line `__RCSID__ = '$Id$'` at the top of your script (I don't know what this does but it says to do it in the docs -BHL)
- Remember that you should **NOT** use print (import gLogger and use its various levels), and you should use dirac's S_OK and exit methods for return codes etc.
