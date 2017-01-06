package IGSP;

import java.util.ArrayList;

public class IGSPUtils
{
	public static ArrayList<Sequence> genCandidate(ArrayList<Sequence> l) // 由Lk生成Ck+1
	{
		ArrayList<Sequence> c=new ArrayList<Sequence>();

		// 对于种子集L进行连接操作
		for (int i = 0; i < l.size(); i++)
		{
			for (int j = i; j < l.size(); j++)
			{
				if (i != j)
				{
					// System.out.println("下面是lk.i序列和lk.j序列连接"+lk.size());;
					Sequence temp=new Sequence();
					if ((temp=joinAndInsert(l.get(i), l.get(j))) != null)
						c.add(temp);
					// System.out.println("下面是lk.j序列和lk.i序列连接---------------"+lk.get(i)+lk.get(j));;
					if ((temp=joinAndInsert(l.get(j), l.get(i))) != null)
						c.add(temp);
					// System.out.println("下面是lk.j序列和lk.i序列连接---------------"+lk.get(j)+lk.get(i));;
				}
			}
		}
		cut(c,l);
		return c;
	}
	private static Sequence joinAndInsert(Sequence s1, Sequence s2)
	{
		// System.out.println("开始了joinAndInsert");;
		if (s1.size() == 1)// l=1时候单独讨论
		{
			Sequence result=new Sequence();
			// System.out.println("l的长度为1");
			result.addElement(s1.getElement(0));
			result.addElement(s2.getElement(0));
			return result;
		} else
		{
			// 去除第一个元素
			// System.out.println("去除第一个元素前，s1:"+s1);
			Sequence first = s1.removeFirstElement();
			// System.out.println("去除第一个元素后，s"+s);
			// 去除最后一个元素
			// System.out.println("去除第二个元素前，s2:"+s2);
			Sequence second = s2.removeLastElement();
			// System.out.println("去除第二个元素前，st:"+st);
			if (first.equals(second))
			{
				Sequence result=new Sequence(s1);
				result.addElement(s2.getElement(s2.size() - 1));
				return result;
			} else
				return null;
		}
	}
	private static void cut(ArrayList<Sequence> c,ArrayList<Sequence> l) // Ck剪枝
	{
		for (int i = 0; i < c.size(); i++)
		{
			boolean flag = false;	
			for (int j = 0; j < c.get(i).size(); j++)
			{
				Sequence temp = new Sequence(c.get(i));
				temp.removeElement(j);
				if (temp.notInSequence(l))
				{
					
					flag = true;
				}
				if (flag)
				{
					c.remove(i);
					i--;
					break;
				}
			}
		}
	}
}
