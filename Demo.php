<?php
/**
 * 基础控制器
 */
namespace App;
use App\Model\Redis;

class Controller
{
    public $errorMsg = ''; // 错误消息
    public $fileName = 'log.log';
    public function __construct()
    {
        $this->__checkRedis();
    }

    /**
     * Redis服务检测
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:50 下午
     */
    private function __checkRedis()
    {
        if(!class_exists('Redis')) {
            exit('Redis ext not found!');
        }
        $config = [
            'host'      => '127.0.0.1',
            'port'      => 6379,
            'password'  => '',
        ];
        $attr = [
            'dbIndex' => 0,
            'timeOut' => 30,
        ];
        try {
            $Redis = Redis::getInstance($config,$attr);
            $Redis->hSetA('sunxs','name',time());
        }catch (\Exception $exception) {
            exit($exception->getMessage());
        }
    }

    /**
     * 成功返回
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 7:24 下午
     * @param $data
     * @return array
     */
    protected function responseSucc($data)
    {
        echo json_encode(['stat' => 1, 'data' => $data],256);
    }

    /**
     * 设置错误信息
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 7:58 下午
     * @param string $msg
     */
    public function setError($msg='')
    {
        $this->errorMsg = $msg;
    }

    /**获取错误信息
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 7:58 下午
     * @return string
     */
    public function getError()
    {
        return $this->errorMsg;
    }

    /**
     * 记录文件日志
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 4:01 下午
     * @param $content
     * @param string $fileName
     * @return bool
     */
    public function log($content,$fileName='')
    {
        if(empty($content)) return false;
        if(empty($fileName)) $fileName = $this->fileName;

        if(is_array($content) || is_object($content)) {
            file_put_contents($fileName,print_r($content,true) ,FILE_APPEND);
        }else{
            file_put_contents($fileName,$content . PHP_EOL ,FILE_APPEND);
        }

        return true;
    }
}

/**
 * 接口类
 * Class BaseController
 */
namespace App;

abstract class  BaseController  extends Controller
{
    protected $_time;
    protected $_config;

    protected $m = null;
    public function __construct()
    {
        parent::__construct();
        $this->_time    = time();
        $this->_config  = $this->getConfig();

        if(!$this->_config['redisModelPath']) {
            throw new \Exception("Miss redisModelPath");
        }
        // 加载这个实例
        $this->m = new $this->_config['redisModelPath'];
    }

    /**
     * 获取配置文件
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 10:14 上午
     * @return mixed
     */
    protected abstract function getConfig();

    /**
     * 消费成功后处理
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 10:17 上午
     * @param $id
     * @return mixed
     */
    protected abstract function succeed($id);

    /**
     * 任务报警系统
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 10:21 上午
     * @param $str
     * @return mixed
     */
    protected abstract function dealBlock($str);


    /**
     * 添加数据到队列
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 11:49 上午
     * @param $id
     * @return array
     */
    public function publish($id)
    {
        $res = $this->m->publish('', $id);
        return !empty($res['result']) ? ['stat' => 1] : ['stat' => 0];
    }

    /**
     * 队列消费者（发送短信、发送邮件等任务的入口）
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 10:14 上午
     * @return bool
     */
    public function consume()
    {
        // get_class_methods($this)
        $className = $this->getClassName();
        // 将失败队列数据转移到预执行队列( All right data  ==> left queue)
        $this->mvRList();
        while (true){
            // 读取预执行队列数据 (从 left queue 获取一个数据)
            $res = $this->m->consumeReady();
            if(empty($res)){
                echo $className . "预执行队列执行完成...\r\n\r\n";
                return false;
            }
            // 具体的任务实现
            $result = $this->doMethod($res);
            if($result) {
                // 执行成功的回调
                $this->succeed($res);
            }else{
                // 加入到对应的回滚队列
                $this->m->goBack($res);
            }

            $incrStringParam = $result . ',' . $this->_time;
            // 单条消息重试次数上限阈值 推送报警
            if($this->dealBlock($incrStringParam) == false) {
                exit($className . '执行失败~推送报警');
            }
        }
    }

    /**
     * 安全队列转移
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 10:26 上午
     */
    private function mvRList()
    {
        while (true){
            $result = $this->m->goBack();
            if(empty($result)){
                return ;
            }
        }
    }

    /**
     * 获取调用类
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 4:09 下午
     * @return mixed
     */
    private function getClassName()
    {
        $str = get_called_class();
        $classNameRes = explode('\\',$str);
        return array_pop($classNameRes);
    }

}

class sendMsg extends BaseController
{
    /**
     * 推送短信基础类
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:43 下午
     * @return array|mixed
     */
    protected function getConfig()
    {
        return [
            'redisModelPath'    => 'App\\Fcenter\\sendMsgQueueRedis',
            'counter'           => 'sxs_open_sendMsg_', // 错误统计key
        ];
    }

    /**
     * 业务逻辑
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:42 下午
     * @param $res
     * @return bool
     */
    public function doMethod($res)
    {
        // 根据订单id 查询订单信息
        // 判断订单状态
        // 获取手机号
        // 调用短信组件
        // 执行发送短信
        if(empty($res)) {
            $this->setError('没有找到执行任务信息');
            return false;
        }
        $this->execSendMsg();
        echo "订单ID{$res}发送短信成功~\r\n";
        return true;

    }

    /**
     * 推送短信方法
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:42 下午
     * @return bool
     */
    public function  execSendMsg()
    {
        return true;
    }

    /**
     * 短信推送成功后回调
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:42 下午
     * @param $id
     * @return mixed
     */
    public function succeed($id)
    {

        // 将这个数据加入到 发送邮件队列任务中
        return $this->m->goPrintQueue($id);
    }

    /**
     * 钉钉消息推送
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 8:35 下午
     * @param $str
     * @return bool|mixed
     */
    public function dealBlock($str)
    {
        if ($this->m->incrNumKey($this->_config['counter'] . $str) > 3) {
            // 调用curl 推送报警消息
            echo "钉钉消息推送成功\r\n";
            return false;
        }

        return true;
    }
}
namespace App;
class sendMail extends  BaseController
{
    /**
     * 获取推送邮件自定义配置
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:28 下午
     * @return array|mixed
     */
    protected function getConfig()
    {
        return [
            'redisModelPath'    => 'App\\Fcenter\\sendMailQueueRedis',
            'counter'           => 'sxs_open_sendMail_',
        ];
    }

    /**
     * 执行推送邮件任务
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:22 下午
     * @param $res
     * @return bool
     */
    public function doMethod($res)
    {
        if(empty($res)) {
            $this->setError("发送邮件队列没有找到执行任务信息\r\n");
            return false;
        }
        // 查询订单信息
        // 发送邮件
        $this->execSendMail();
        echo "订单ID{$res}发送邮件成功~\r\n";
        return true;

    }

    /**
     * 邮件推送成功 队列中删除数据
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:22 下午
     * @param $id
     * @return mixed
     */
    public function succeed($id)
    {
        // 从队列中删除数据
         $delRes = $this->m->lremList($id);
         return $delRes;

    }

    /**
     * 邮件发送失败执行推送
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:26 下午
     * @param $str
     * @return bool|mixed
     */
    public function dealBlock($str)
    {
        // 邮件报错十次 推送报警
        if ($this->m->incrNumKey($this->_config['counter'] . $str) > 10) {
            // 调用curl 推送报警消息
            echo "钉钉消息推送成功\r\n";
            return false;
        }
        return true;
    }

    /**业务逻辑执行推送邮件
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:28 下午
     * @return bool
     */
    private function execSendMail()
    {
        return true;
    }
}


namespace App;
/**
 * 订单生成系统
 * Class Order
 * @package App
 */
class Order extends Controller
{
    /**
     * 生成订单
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 3:05 下午
     * @throws \Exception
     */
    public function makeOrder()
    {
        $num = 2;
        for ($i=1;$i<=$num;$i++){
            $orderInfo = rand(1000,9999);
            if (!$this->pushQueueMsg($orderInfo)){
                // 失败加入日志
                $this->log($this->getError());
            };
        }
        $msg = "系统生成{$num}个订单成功";
        $this->responseSucc($msg);
    }


    /**
     * 订单消息加入队列
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 11:55 上午
     * @param string $msg
     * @return bool
     * @throws \Exception
     */
    private function pushQueueMsg($msg='')
    {
        // 支持批量插入
        if(empty($msg)){
            $this->setError('消息体为空');
            return false;
        }
        if(!is_numeric($msg)) {
            $this->setError('消息体结构必须是整型');
            return false;
        }
        $invoiceOpen = new sendMsg();
        if(!$invoiceOpen->publish($msg)){
            $this->setError('加入消息失败');
            return false;
        };

        return true;
    }
}

/**
 * Redis基础操作类
 * Class Redis
 */
namespace  App\Model;
class Redis
{

    //当前数据库ID号
    protected  $dbId=0;
    protected  $timeout = 0;
    // Redis连接句柄
    private $redis;
    /**
     * 实例化的对象,单例模式.
     */
    private static $_instance = null; //静态实例

    //连接属性数组
    protected $attr=array(
        //连接超时时间，redis配置文件中默认为300秒
        'timeout'   => 30,
        //选择的数据库
        'dbIndex'     => 0,
    );

    //什么时候重新建立连接
    protected $expireTime;

    protected $host;

    protected $port;

    private function __construct($config,$attr=array())
    {
        $this->attr        = array_merge($this->attr,$attr);
        $this->port        = $config['port'] ? $config['port'] : 6379;
        $this->host        = $config['host'];
        $this->dbId        = $this->attr['dbIndex'];
        $this->timeout     = $this->attr['timeout'];

        try {
            $this->redis = new \Redis();
            $this->redis->connect($this->host, $this->port, $this->timeout);
            if(isset($config['password']) && !empty($config['password']))
            {
                $this->redis->auth($config['password']);
            }
            // 切换数据库类
            $this->redis->select($this->dbId);
        }catch (\Exception $exception) {
            throw new \Exception("Redis Connect Failed," . $exception->getMessage());
        }
    }

    /**
     * Redis的单利模式
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 11:54 上午
     * @param $config
     * @param array $attr
     * @return Redis|null
     * @throws \Exception
     */
    public static function getInstance($config, $attr = array())
    {
       if (! (self::$_instance instanceof self)) {
            self::$_instance = new self($config,$attr);
        }

        return self::$_instance;
    }

    /**
     * redis 防止克隆
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 11:33 上午
     */
    private function __clone(){}

    /*****************hash表操作函数*******************/

    /**
     * 得到hash表中一个字段的值
     * @param string $key 缓存key
     * @param string  $field 字段
     * @return string|false
     */
    public function hGet($key,$field)
    {
        return $this->redis->hGet($key,$field);
    }

    /**
     * 为hash表设定一个字段的值
     * @param string $key 缓存key
     * @param string  $field 字段
     * @param string $value 值。
     * @return bool
     */
    public function hSetA($key,$field,$value)
    {
        return $this->redis->hSet($key,$field,$value);
    }

    /**
     * 判断hash表中，指定field是不是存在
     * @param string $key 缓存key
     * @param string  $field 字段
     * @return bool
     */
    public function hExists($key,$field)
    {
        return $this->redis->hExists($key,$field);
    }

    /**
     * 删除hash表中指定字段 ,支持批量删除
     * @param string $key 缓存key
     * @param string  $field 字段
     * @return int
     */
    public function hdel($key,$field)
    {
        $fieldArr=explode(',',$field);
        $delNum=0;

        foreach($fieldArr as $row)
        {
            $row=trim($row);
            $delNum+=$this->redis->hDel($key,$row);
        }

        return $delNum;
    }

    /**
     * lua原子性操作
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 8:12 下午
     * @param $str
     * @param $array
     * @param $index
     * @return mixed
     */
    public function evalLua($str,$array,$index)
    {
        return $this->redis->eval($str,$array,$index);
    }

    /**
     * 字符串增加指定的步长
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 8:11 下午
     * @param $str
     * @param int $step
     * @return int
     */
    public function incrNum($str,$step = 1)
    {
        return $this->redis->incrBy($str,$step);
    }

    /**
     * Redis设置缓存的超时时间
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:30 下午
     * @param $key
     * @param $time
     */
    public function setExpire($key,$time)
    {
        $this->redis->expire($key,$time);
    }

}


namespace App\Fcenter;
use App\Model\Redis;

/**
 * Class ReliableQueue
 * @package App\Fcenter
 * @return ReliableQueue
 */
abstract class ReliableQueue extends Redis
{

    protected $_queue;
    protected $_rList;
    protected $_lList;
    protected $_time;
    private $_configStr;

    const HASH_KEY = 'sunxs_list';

    public function __construct()
    {
        $this->_time = time();
        $this->_configStr = $this->getConfig();
        $this->_queue = Redis::getInstance($this->_configStr['config'],$this->_configStr['attr']);
        $this->_lList = $this->_configStr['key'];
        $this->_rList = $this->_configStr['rListKey'];
    }

    abstract protected function getConfig();


    /**
     * 从预执行队列中获取数据
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 4:34 下午
     * @return mixed
     * @return ReliableQueue
     */
    public function consumeReady()
    {
        $lua = <<<LUA
return redis.call("RPOPLPUSH",KEYS[2],KEYS[3])
LUA;
        return   $this->_queue->evalLua($lua,[self::HASH_KEY, $this->_lList, $this->_rList],3);
    }

    /**
     * 队列数据流转
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 7:38 下午
     * @return mixed
     */
    public function goBack()
    {
        $lua = <<<LUA
return redis.call("RPOPLPUSH",KEYS[2],KEYS[3])
LUA;
        return  $this->_queue->evalLua($lua,[self::HASH_KEY, $this->_rList, $this->_lList],3);
    }

    /**
     * 设置key的错误次数
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 8:18 下午
     * @param $configStr
     * @return bool|int
     */
    public function incrNumKey($configStr)
    {
        $res = $this->_queue->incrNum($configStr);
        if ($res === 1) {
            $this->_queue->setExpire($configStr, 60);
            return $res;
        }

        return false;
    }

    /**
     * 将右侧队列数据加入预执行队列
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:40 下午
     * @return mixed
     */
    public function mvRList()
    {
        $lua = <<<LUA
return redis.call("RPOPLPUSH",KEYS[2],KEYS[3])
LUA;
        return  $this->_queue->evalLua($lua,[self::HASH_KEY, $this->_rList, $this->_lList],3);
    }

    /**左右队列数据交换
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-21 2:40 下午
     * @param string $id
     * @param $value
     * @return mixed
     */
    public function publish($id = '', $value)
    {
        if(empty($id)){
            $id = $this->_lList;
        }
        $lua = <<<LUA
return redis.call("lPush",KEYS[2],ARGV[1])
LUA;
        return   $this->_queue->evalLua($lua,[self::HASH_KEY, $id, $value],2);
    }
}


namespace App\Fcenter;
use App\sendMsg;
use App\Order;
use App\sendMail;

class sendMsgQueueRedis extends ReliableQueue
{
    /**
     * 获取请求申请发票Redis数据
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 4:39 下午
     */
    public function getConfig()
    {
        return [
            'key'           => 'baiwang_invoice_open_left_list',
            'rListKey'      => 'baiwang_invoice_open_right_list',
            'type'          => 'list',
            'config'        => [
                    'host'      => '127.0.0.1',
                    'port'      => 6379,
                    'password'  =>'',
            ],
            'attr'          => [
                    'dbIndex' => 0,
                    'timeOut' => 30,
            ],
        ];
    }

    /**
     * 开发发票加入到打印发票队列中
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 4:37 下午
     */
    public function goPrintQueue()
    {
        $config = [
            'key' => 'sunxs_send_mail_left_list',
            'rListKey' => 'sunxs_send_mail_right_list',
        ];
        $key = $config['key'];

        $lua = <<<LUA
return redis.call("RPOPLPUSH",KEYS[2],KEYS[3])
LUA;
        return   $this->_queue->evalLua($lua,[self::HASH_KEY, $this->_rList,$key],3);
    }


}

/**
 * 推送邮件队列
 * Class sendMailQueueRedis
 * @package App\Fcenter
 */
class sendMailQueueRedis extends ReliableQueue
{
    /**
     * 获取请求申请发票Redis数据
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 4:39 下午
     */
    public function getConfig()
    {
        return [
            'key'           => 'sunxs_send_mail_left_list',
            'rListKey'      => 'sunxs_send_mail_right_list',
            'config'        => [
                'host'      => '127.0.0.1',
                'port'      => 6379,
                'password'  =>'',
            ],
            'attr'          => [
                'dbIndex' => 0,
                'timeOut' => 30,
            ],
        ];
    }

    /**
     * 队列删除数据
     * @auth sunxs <3762820@qq.com>
     * @date 2020-06-20 4:37 下午
     */
    public function lremList($id)
    {
        $config = [
            'key'       => 'sunxs_send_mail_left_list',
            'rListKey'  => 'sunxs_send_mail_right_list',
        ];
        $key = $config['rListKey'];
        $lua = <<<LUA
return redis.call("lrem",KEYS[2],ARGV[1],ARGV[2])
LUA;
        return $this->_queue->evalLua($lua,[self::HASH_KEY,$key,0,$id],2);
    }
}









    /************************************************************************
     *
     *  Instruction for use：
     *      First: You can make order And insert data to Redis
     *      Second:You can perform the function of sending SMS and email
     *  Happy use
     *  Auther：孙先水
     *  Mail:3762820@qq.com
     *************************************************************************/


    /** Step 1 : make Order */
    $order = new Order();
    $order->makeOrder();

    /** Step 2 :Consume Message */
    $obj = new sendMsg();
    $obj->consume();

    /** Step 3: Consume Mail */
    $obj = new sendMail();
    $obj->consume();







