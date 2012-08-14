<?php

namespace ZfcBase\Mapper;

use Zend\Db\Adapter\Adapter;
use Zend\Db\Adapter\Driver\ResultInterface;
use Zend\Db\ResultSet\HydratingResultSet;
use Zend\Db\Sql\Select;
use Zend\Db\Sql\Sql;
use Zend\Db\Sql\Predicate\Predicate;
use Zend\Db\Sql\Expression;
use Zend\Stdlib\Hydrator\HydratorInterface;
use Zend\Stdlib\Hydrator\ClassMethods;
use ZfcBase\EventManager\EventProvider;
use ZfcBase\Db\Adapter\MasterSlaveAdapterInterface;

abstract class AbstractDbMapper extends EventProvider
{
    /**
     * @var Adapter
     */
    protected $dbAdapter;

    /**
     * @var Adapter
     */
    protected $dbSlaveAdapter;

    /**
     * @var HydratorInterface
     */
    protected $hydrator;

    /**
     * @var object
     */
    protected $entityPrototype;

    /**
     * @var HydratingResultSet
     */
    protected $resultSetPrototype;

    /**
     * @var Select
     */
    protected $selectPrototype;

    /**
     * @var string name of default table. Will be used when null is passed for the table name in methods
     */
    protected $tableName;

    /**
     * @param Adapter $dbAdapter
     * @param HydratorInterface $hydrator
     */
    public function __construct(Adapter $dbAdapter = null, $hydrator = null){
        if (null !== $dbAdapter){
            $this->setDbAdapter($dbAdapter);
        }
        if (null !== $hydrator){
            $this->setHydrator($hydrator);
        }
    }

    /**
     * return the row count for a table or an array of predicates
     * @param array|Predicate $where
     * @param string $tableName optional table name to perform count on
     *
     * @return int
     */
    public function count($where = null, $tableName = null){
        $tableName = $tableName ?: $this->tableName;

        $select = new Select($tableName);

        if ($where instanceof Predicate){
            $select->where($where);
        } elseif (is_array($where)){
            foreach ($where as $wh){
                $select->where($wh);
            }
        }

        $select->columns(array('c' => new Expression('count(*)')));
        $stmt = $this->getDbSlaveAdapter()->query($select->getSqlString());
        $resultSet = $stmt->execute();
        $row = $resultSet->current();

        return (int)$row['c'];
    }

    /**
     * a basic select
     * @param array|Predicate $where
     * @param string $tableName
     *
     * @return HydratingResultSet
     */
    public function select($where, $tableName = null){
        $tableName = $tableName ?: $this->tableName;
        $select = new Select($tableName);

        if ($where instanceof Predicate){
            $select->where($where);
        } elseif (is_array($where)){
            foreach ($where as $wh){
                $select->where($wh);
            }
        }

        $stmt = $this->getDbSlaveAdapter()->query($select->getSqlString());
        $result = $stmt->execute();

        $resultSet = $this->getResultSet();

        if (isset($entityPrototype)) {
            $resultSet->setObjectPrototype($entityPrototype);
        }

        if (isset($hydrator)) {
            $resultSet->setHydrator($hydrator);
        }

        $resultSet->initialize($result);

        return $resultSet;
    }

    /**
     * @param Select $select
     * @return HydratingResultSet
     */
    public function selectWith(Select $select, $entityPrototype = null, HydratorInterface $hydrator = null)
    {
        $adapter = $this->getDbSlaveAdapter();
        $statement = $adapter->createStatement();
        $select->prepareStatement($adapter, $statement);
        $result = $statement->execute();

        $resultSet = $this->getResultSet();

        if (isset($entityPrototype)) {
            $resultSet->setObjectPrototype($entityPrototype);
        }

        if (isset($hydrator)) {
            $resultSet->setHydrator($hydrator);
        }

        $resultSet->initialize($result);

        return $resultSet;
    }

    /**
     * @param object|array $entity
     * @param string $tableName
     * @param HydratorInterface $hydrator
     * @return ResultInterface
     */
    public function insert($entity, $tableName = null, HydratorInterface $hydrator = null)
    {
        $tableName = $tableName ?: $this->tableName;

        $rowData = $this->entityToArray($entity, $hydrator);

        $sql = new Sql($this->getDbAdapter(), $tableName);
        $insert = $sql->insert();
        $insert->values($rowData);

        $statement = $sql->prepareStatementForSqlObject($insert);
        return $statement->execute();
    }

    /**
     * @param object|array $entity
     * @param  string|array|closure $where
     * @param string $tableName
     * @param HydratorInterface $hydrator
     * @return mixed
     */
    public function update($entity, $where, $tableName = null, HydratorInterface $hydrator = null)
    {
        $tableName = $tableName ?: $this->tableName;

        $rowData = $this->entityToArray($entity, $hydrator);
        $sql = new Sql($this->getDbSlaveAdapter(), $tableName);

        $update = $sql->update();
        $update->set($rowData);
        $update->where($where);
        $statement = $sql->prepareStatementForSqlObject($update);
        $result = $statement->execute();

        return $result->getAffectedRows();
    }

    /**
     * helper method to begin a transaction
     */
    public function beginTransaction(){
        $this->getDbSlaveAdapter()->driver->getConnection()->beginTransaction();
    }

    /**
     * helper method to commit
     */
    public function commit(){
        $this->getDbSlaveAdapter()->driver->getConnection()->commit();
    }

    /**
     * helper method to rollback
     */
    public function rollback(){
        $this->getDbSlaveAdapter()->driver->getConnection()->getConnection()->rollback();
    }

    /**
     * @return object
     */
    public function getEntityPrototype()
    {
        return $this->entityPrototype;
    }

    /**
     * @param object $modelPrototype
     * @return AbstractDbMapper
     */
    public function setEntityPrototype($entityPrototype)
    {
        $this->entityPrototype = $entityPrototype;
        $this->resultSetPrototype = null;
        return $this;
    }

    /**
     * @return Adapter
     */
    public function getDbAdapter()
    {
        return $this->dbAdapter;
    }

    /**
     * @param Adapter $dbAdapter
     * @return AbstractDbMapper
     */
    public function setDbAdapter(Adapter $dbAdapter)
    {
        $this->dbAdapter = $dbAdapter;
        if ($dbAdapter instanceof MasterSlaveAdapterInterface) {
            $this->setDbSlaveAdapter($dbAdapter->getSlaveAdapter());
        }
        return $this;
    }

    /**
     * @return Adapter
     */
    public function getDbSlaveAdapter()
    {
        return $this->dbSlaveAdapter ?: $this->dbAdapter;
    }

    /**
     * @param Adapter $dbAdapter
     * @return AbstractDbMapper
     */
    public function setDbSlaveAdapter(Adapter $dbSlaveAdapter)
    {
        $this->dbSlaveAdapter = $dbSlaveAdapter;
        return $this;
    }

    /**
     * @return HydratorInterface
     */
    public function getHydrator()
    {
        if (!$this->hydrator) {
            $this->hydrator = new ClassMethods(false);
        }
        return $this->hydrator;
    }

    /**
     * @param HydratorInterface $hydrator
     * @return AbstractDbMapper
     */
    public function setHydrator(HydratorInterface $hydrator)
    {
        $this->hydrator = $hydrator;
        $this->resultSetPrototype = null;
        return $this;
    }

    /**
     * @return HydratingResultSet
     */
    protected function getResultSet()
    {
        if (!$this->resultSetPrototype) {
            $this->resultSetPrototype = new HydratingResultSet;
            $this->resultSetPrototype->setHydrator($this->getHydrator());
            $this->resultSetPrototype->setObjectPrototype($this->getEntityPrototype());
        }
        return clone $this->resultSetPrototype;
    }

    /**
     * select
     *
     * @return Select
     */
    protected function getSelectForPrototype()
    {
        if (!$this->selectPrototype) {
            $this->selectPrototype = new Select;
        }
        return clone $this->selectPrototype;
    }

    /**
     * Uses the hydrator to convert the entity to an array.
     *
     * Use this method to ensure that you're working with an array.
     *
     * @param object $entity
     * @return array
     */
    protected function entityToArray($entity, HydratorInterface $hydrator = null)
    {
        if (is_array($entity)) {
            return $entity; // cut down on duplicate code
        } elseif (is_object($entity)) {
            if (!$hydrator) {
                $hydrator = $this->getHydrator();
            }
            return $hydrator->extract($entity);
        }
        throw new Exception\InvalidArgumentException('Entity passed to db mapper should be an array or object.');
    }
}
