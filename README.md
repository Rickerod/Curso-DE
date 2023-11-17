<h1>CoderDataEngineering</h1>

<h2>Consideraciones</h2>

<p>El DAG funcional se denomina "Entregable_TAG".</p>
<p>El DAG Entregable_TAG_DOperator no se logró ejecutar debido a un problema de permisos en windows 10.</p>

<h2>Instrucciones</h2>

<p>Para ejecutar el DAG con éxito, es necesario definir las siguientes variables de entorno en Airflow:</p>

<ol>
  <li><code>user_redshift</code>: Usuario para la conexión a Redshift.</li>
  <li><code>secret_pass_redshift</code>: Contraseña para la conexión a Redshift.</li>
  <li><code>email_user</code>: Usuario del correo electrónico para notificaciones.</li>
  <li><code>email_pass</code>: Contraseña del correo electrónico para notificaciones.</li>
</ol>

<p>Para ejecutar el contenedor, es necesario un archivo <code>.env</code> con las siguientes variables de entorno:</p>

<h3>Credenciales de Redshift:</h3>
<ul>
  <li><code>URL</code>: URL de conexión a la base de datos Redshift.</li>
  <li><code>DATA_BASE</code>: Nombre de la base de datos en Redshift.</li>
  <li><code>USER</code>: Usuario de Redshift.</li>
  <li><code>PWD</code>: Contraseña de Redshift.</li>
  <li><code>PORT</code>: Puerto de conexión a Redshift.</li>
</ul>

<h3>Credenciales del correo electrónico:</h3>
<ul>
  <li><code>USER_EMAIL</code>: Usuario del correo electrónico para notificaciones.</li>
  <li><code>USER_PASS</code>: Contraseña del correo electrónico para notificaciones.</li>
</ul>

<p>Estas variables son esenciales para garantizar una ejecución exitosa del DAG y del contenedor.</p>



