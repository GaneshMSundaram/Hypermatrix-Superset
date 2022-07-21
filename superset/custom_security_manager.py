from os import environ
from flask_appbuilder.security.views import expose
from flask_appbuilder.security.sqla.manager import SecurityManager
from flask_appbuilder.security.manager import BaseSecurityManager
from flask_appbuilder.security.manager import AUTH_REMOTE_USER
from flask_login import login_user
from flask import redirect, g, flash, request
from superset.security.manager import SupersetSecurityManager

# Create a custom view to authenticate the user
AuthRemoteUserView=BaseSecurityManager.authremoteuserview
class SupersetAuthRemoteUserView(AuthRemoteUserView):
    @expose('/login/')
    def login(self):
      print("This is LOGIN")

      username = request.headers.get('session')
      user = self.appbuilder.sm.find_user('admin')
      login_user(user, remember=False)

      user = environ.pop('HTTP_X_PROXY_REMOTE_USER', None)
      # environ['REMOTE_USER'] = user
      print(user)
      roles = g.user.roles
      # Can also get redirect url after parsing url 
      redirect_url = '/superset/welcome/'
      return redirect(redirect_url)
    
    # def load_user(self, pk):
    #     user = self.get_user_by_id(int(pk))
    #     print("Hiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii9999999999657576599999", user)
    #     return self.get_user_by_id(int(pk))

    # def load_user_jwt(self, pk):
    #     user = self.load_user(pk)
    #     # Set flask g.user to JWT user, we can't do it on before request
    #     print("Hiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii999999999999999", user)
    #     g.user = user
    #     return user


# Create a custom Security manager that override the authremoteuserview above
class CustomSecurityManagerSameFile(SupersetSecurityManager):
    authremoteuserview = SupersetAuthRemoteUserView
  
class UserDemo: 
  def __init__(self, firstName, lastName):
    self.first_name = firstName
    self.last_name = lastName
    self.is_active = True