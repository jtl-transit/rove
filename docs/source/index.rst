Welcome to Rove's documentation!
===================================

**ROVE** is an interactive bus transit performance analysis tool developed by the MIT Transit Lab.

ROVE is intended to be a flexible and user-friendly dashboard to visualize and compare the performance 
of a bus network over time. It is generalizable so that it can be easily adapted to different networks. 
It includes performance metrics related to scheduled supply, actual supply and passenger loading, based 
on the data available from the agency. It can be used for service planning, scheduling, detour 
planning and many other applications. It is *browser-based*, and therefore does not require any special 
software or advanced technical knowledge on the part of the end user.

Visit the `ROVE repo on Github <https://github.com/jtl-transit/rove>`_ to download the source code.

Check out the :doc:`quick_start` section for instructions on how to get started.

.. note::

   The current implementation of ROVE provides stable support for static GTFS data only. Although ROVE 
   has the capability to support AVL data, we cannot guarantee that your data will work perfectly in ROVE, due to 
   the lack of standardization for AVL data. Users wishing to display observed metrics calculated with AVL 
   data may do so at your own discretion. More information will be provided in the future on advanced topics such as 
   working with AVL data, adding customized metric calculations, editing configuration files, etc.

Contents
--------

.. toctree::
   :maxdepth: 2

   quick_start
   backend
   frontend

Contributors
--------

The project was initially developed by Ru Mehendale. 

Past contributors whose contributions were instrumental in making this project possible are 
Nick Caros (caros@mit.edu) and Xiaotong Guo (xtguo@mit.edu).


Contact Us
--------
Currently, the project is being actively maintained and improved by 
Yuzhu Huang (yuzhuh@mit.edu) and Yen-Chu Wu (yenchuwu@mit.edu).

For inquiries related to copyright or research collaborations, please reach out to Anson Stewart (ansons@mit.edu).