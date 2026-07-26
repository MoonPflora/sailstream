
Issueues/

when ordering :
WHen user asks for an item and its 100% mach with item name the intent is written as order confirmation which is wrong , since we always have to asks confirmation of product before ordering happens , so when user asks about an item and its fast match , and send product info , and user says i want that , we asks if thats the product he means if he says yes , we send order template ( asks for name/phone number and city and adress and quantity ) when user sends that tempalte adn its validated then we send order confitmation , sending the order total and info and deliviery stats adn ask user to say confirm to send then make the order.
  compound case : sometimes user says i want / ordrer item[matched searched] we dont immedietly jump for order creation adn escalate , it could create infinite loops . we go to product confirmation as before .
must check ordering / browsing / product confitmation loop so it doesnt create a deadlock and an infinite loop .
case : what if a product is attached to the json but user asks for something else ?: ignore that product attached and go to regular route using that product if it was found.