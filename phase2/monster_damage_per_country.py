{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Dash is running on http://127.0.0.1:8050/\n",
      "\n",
      " * Serving Flask app '__main__'\n",
      " * Debug mode: on\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "Traceback (most recent call last):\n",
      "  File \"/usr/local/lib/python3.7/site-packages/ipykernel_launcher.py\", line 17, in <module>\n",
      "    app.launch_new_instance()\n",
      "  File \"/usr/local/lib/python3.7/site-packages/traitlets/config/application.py\", line 1042, in launch_instance\n",
      "    app.initialize(argv)\n",
      "  File \"/usr/local/lib/python3.7/site-packages/traitlets/config/application.py\", line 113, in inner\n",
      "    return method(app, *args, **kwargs)\n",
      "  File \"/usr/local/lib/python3.7/site-packages/ipykernel/kernelapp.py\", line 666, in initialize\n",
      "    self.init_sockets()\n",
      "  File \"/usr/local/lib/python3.7/site-packages/ipykernel/kernelapp.py\", line 307, in init_sockets\n",
      "    self.shell_port = self._bind_socket(self.shell_socket, self.shell_port)\n",
      "  File \"/usr/local/lib/python3.7/site-packages/ipykernel/kernelapp.py\", line 244, in _bind_socket\n",
      "    return self._try_bind_socket(s, port)\n",
      "  File \"/usr/local/lib/python3.7/site-packages/ipykernel/kernelapp.py\", line 220, in _try_bind_socket\n",
      "    s.bind(\"tcp://%s:%i\" % (self.ip, port))\n",
      "  File \"/usr/local/lib64/python3.7/site-packages/zmq/sugar/socket.py\", line 232, in bind\n",
      "    super().bind(addr)\n",
      "  File \"zmq/backend/cython/socket.pyx\", line 568, in zmq.backend.cython.socket.Socket.bind\n",
      "  File \"zmq/backend/cython/checkrc.pxd\", line 28, in zmq.backend.cython.checkrc._check_rc\n",
      "zmq.error.ZMQError: Address already in use\n"
     ]
    },
    {
     "ename": "SystemExit",
     "evalue": "1",
     "output_type": "error",
     "traceback": [
      "An exception has occurred, use %tb to see the full traceback.\n",
      "\u001b[0;31mSystemExit\u001b[0m\u001b[0;31m:\u001b[0m 1\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "/usr/local/lib/python3.7/site-packages/IPython/core/interactiveshell.py:3561: UserWarning:\n",
      "\n",
      "To exit: use 'exit', 'quit', or Ctrl-D.\n",
      "\n"
     ]
    }
   ],
   "source": [
    "import dash\n",
    "from dash import dcc, html\n",
    "import plotly.express as px\n",
    "import pandas as pd\n",
    "\n",
    "# Sample monster data - replace this with your data\n",
    "data = {\n",
    "    \"country\": [\"USA\", \"Canada\", \"UK\"],\n",
    "    \"monster_name\": [\"Dragon\", \"Goblin\", \"Troll\"],\n",
    "    \"damage\": [10000, 5000, 15000]\n",
    "}\n",
    "df = pd.DataFrame(data)\n",
    "\n",
    "# Create the Dash app\n",
    "app = dash.Dash(__name__)\n",
    "\n",
    "# Define the app layout\n",
    "app.layout = html.Div([\n",
    "    html.H1(\"Monster Attacks\"),\n",
    "    dcc.Graph(\n",
    "        figure=px.bar(df, x='country', y='damage', color='monster_name', title='Monster Damage by Country')\n",
    "    )\n",
    "])\n",
    "\n",
    "# Run the app\n",
    "if __name__ == '__main__':\n",
    "    app.run_server(debug=True)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.16"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
